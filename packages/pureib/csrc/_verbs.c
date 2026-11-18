/*
 * pureib native layer: native InfiniBand (AF_IB) connections through rdma_cm,
 * one reliable-connected queue pair per connection, one registered arena per
 * connection, and a completion channel that asyncio can wait on.
 *
 * No IPoIB, no sockets: the data path is ibv_post_send / ibv_post_recv and the
 * completion queue. The only file descriptors exposed are the rdma_cm event
 * channel and the completion channel, both non-blocking and epoll-friendly.
 */
#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include <endian.h>
#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <string.h>

#include <infiniband/ib.h>
#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>

#define GID_LEN 16
#define MAX_POLL 64
#define CM_MAX_PRIVATE 255

_Static_assert(sizeof(((struct sockaddr_ib *)0)->sib_addr) == GID_LEN,
               "struct ib_addr must be a 16 byte GID");

typedef struct {
    PyObject_HEAD
    struct rdma_event_channel *channel;
    struct rdma_cm_id *id;
    struct ibv_pd *pd;
    struct ibv_comp_channel *comp;
    struct ibv_cq *cq;
    struct ibv_mr *mr;
    Py_buffer view; /* keeps the registered arena alive until close() */
    int has_view;
    uint32_t max_inline;
} CmObject;

static PyTypeObject CmType;

static void set_os_error(const char *what, int err) {
    PyErr_Format(PyExc_OSError, "%s failed: %s (errno %d)", what, strerror(err), err);
}

static int set_nonblock(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) return -1;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

static int build_ib_addr(struct sockaddr_ib *sib, const char *gid, Py_ssize_t glen, uint64_t sid) {
    if (glen != GID_LEN) {
        PyErr_Format(PyExc_ValueError, "GID must be %d bytes, got %zd", GID_LEN, glen);
        return -1;
    }
    memset(sib, 0, sizeof(*sib));
    sib->sib_family = AF_IB;
    memcpy(&sib->sib_addr, gid, GID_LEN);
    sib->sib_pkey = htobe16(0xFFFF);
    sib->sib_sid = htobe64(sid);
    sib->sib_sid_mask = htobe64(~0ULL);
    return 0;
}

#define CHECK_ID(self)                                              \
    do {                                                            \
        if (!(self)->id) {                                          \
            PyErr_SetString(PyExc_ValueError, "connection closed"); \
            return NULL;                                            \
        }                                                           \
    } while (0)

#define CHECK_CQ(self)                                                 \
    do {                                                               \
        if (!(self)->cq || !(self)->id || !(self)->id->qp) {           \
            PyErr_SetString(PyExc_ValueError, "queue pair not setup"); \
            return NULL;                                               \
        }                                                              \
    } while (0)

static void teardown(CmObject *self) {
    /* Ack pending events first: rdma_destroy_id blocks on unacknowledged events. */
    if (self->channel) {
        struct rdma_cm_event *ev;
        while (rdma_get_cm_event(self->channel, &ev) == 0) {
            struct rdma_cm_id *eid = ev->id;
            int is_request = ev->event == RDMA_CM_EVENT_CONNECT_REQUEST;
            rdma_ack_cm_event(ev);
            if (is_request && eid && eid != self->id) {
                /* An unhandled connection request owns its own id: reject it so the peer fails fast. */
                rdma_reject(eid, NULL, 0);
                rdma_destroy_id(eid);
            }
        }
    }
    if (self->id && self->id->qp) rdma_destroy_qp(self->id);
    if (self->mr) ibv_dereg_mr(self->mr);
    if (self->cq) ibv_destroy_cq(self->cq);
    if (self->comp) ibv_destroy_comp_channel(self->comp);
    if (self->pd) ibv_dealloc_pd(self->pd);
    if (self->id) rdma_destroy_id(self->id);
    if (self->channel) rdma_destroy_event_channel(self->channel);
    self->mr = NULL;
    self->cq = NULL;
    self->comp = NULL;
    self->pd = NULL;
    self->id = NULL;
    self->channel = NULL;
    if (self->has_view) PyBuffer_Release(&self->view);
    self->has_view = 0;
}

static void Cm_dealloc(CmObject *self) {
    teardown(self);
    Py_TYPE(self)->tp_free((PyObject *)self);
}

static int Cm_init(CmObject *self, PyObject *args, PyObject *kwds) {
    (void)kwds;
    if (!PyArg_ParseTuple(args, "")) return -1;
    self->channel = rdma_create_event_channel();
    if (!self->channel) {
        set_os_error("rdma_create_event_channel", errno);
        return -1;
    }
    if (rdma_create_id(self->channel, &self->id, NULL, RDMA_PS_IB)) {
        int err = errno;
        rdma_destroy_event_channel(self->channel);
        self->channel = NULL;
        set_os_error("rdma_create_id", err);
        return -1;
    }
    if (set_nonblock(self->channel->fd)) {
        set_os_error("fcntl", errno);
        teardown(self);
        return -1;
    }
    return 0;
}

static PyObject *Cm_fileno(CmObject *self, PyObject *Py_UNUSED(ignored)) {
    CHECK_ID(self);
    return PyLong_FromLong(self->channel->fd);
}

/* Returns (kind, status, private_data_or_None, child_or_None) or None when idle. */
static PyObject *Cm_cm_event(CmObject *self, PyObject *Py_UNUSED(ignored)) {
    CHECK_ID(self);
    struct rdma_cm_event *ev;
    if (rdma_get_cm_event(self->channel, &ev)) {
        if (errno == EAGAIN) Py_RETURN_NONE;
        set_os_error("rdma_get_cm_event", errno);
        return NULL;
    }
    int kind = (int)ev->event;
    int status = ev->status;
    struct rdma_cm_id *eid = ev->id;
    PyObject *priv = NULL;
    if (kind == RDMA_CM_EVENT_CONNECT_REQUEST || kind == RDMA_CM_EVENT_ESTABLISHED ||
        kind == RDMA_CM_EVENT_CONNECT_RESPONSE || kind == RDMA_CM_EVENT_REJECTED) {
        const void *p = ev->param.conn.private_data;
        priv = PyBytes_FromStringAndSize(p ? (const char *)p : "", p ? ev->param.conn.private_data_len : 0);
        if (!priv) {
            rdma_ack_cm_event(ev);
            return NULL;
        }
    }
    int is_request = kind == RDMA_CM_EVENT_CONNECT_REQUEST;
    rdma_ack_cm_event(ev); /* ev is gone after this; eid stays valid */

    CmObject *child = NULL;
    if (is_request) {
        struct rdma_event_channel *nch = rdma_create_event_channel();
        child = (CmObject *)CmType.tp_alloc(&CmType, 0);
        if (!nch || !child) {
            int err = errno;
            if (nch) rdma_destroy_event_channel(nch);
            rdma_reject(eid, NULL, 0);
            rdma_destroy_id(eid);
            Py_XDECREF(priv);
            Py_XDECREF((PyObject *)child);
            set_os_error("rdma_create_event_channel", err);
            return NULL;
        }
        if (rdma_migrate_id(eid, nch) || set_nonblock(nch->fd)) {
            int err = errno;
            rdma_destroy_event_channel(nch);
            rdma_reject(eid, NULL, 0);
            rdma_destroy_id(eid);
            Py_XDECREF(priv);
            Py_DECREF(child);
            set_os_error("rdma_migrate_id", err);
            return NULL;
        }
        child->channel = nch;
        child->id = eid;
    }
    PyObject *priv_o = priv ? priv : Py_NewRef(Py_None);
    PyObject *child_o = child ? (PyObject *)child : Py_NewRef(Py_None);
    return Py_BuildValue("(iiNN)", kind, status, priv_o, child_o);
}

static PyObject *Cm_bind(CmObject *self, PyObject *args) {
    CHECK_ID(self);
    Py_buffer gid;
    unsigned long long sid;
    if (!PyArg_ParseTuple(args, "y*K", &gid, &sid)) return NULL;
    struct sockaddr_ib sib;
    int rc = build_ib_addr(&sib, gid.buf, gid.len, sid);
    PyBuffer_Release(&gid);
    if (rc) return NULL;
    if (rdma_bind_addr(self->id, (struct sockaddr *)&sib)) {
        set_os_error("rdma_bind_addr", errno);
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject *Cm_listen(CmObject *self, PyObject *args) {
    CHECK_ID(self);
    int backlog;
    if (!PyArg_ParseTuple(args, "i", &backlog)) return NULL;
    if (rdma_listen(self->id, backlog)) {
        set_os_error("rdma_listen", errno);
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject *Cm_resolve(CmObject *self, PyObject *args) {
    CHECK_ID(self);
    Py_buffer gid;
    unsigned long long sid;
    int timeout_ms;
    if (!PyArg_ParseTuple(args, "y*Ki", &gid, &sid, &timeout_ms)) return NULL;
    struct sockaddr_ib sib;
    int rc = build_ib_addr(&sib, gid.buf, gid.len, sid);
    PyBuffer_Release(&gid);
    if (rc) return NULL;
    if (rdma_resolve_addr(self->id, NULL, (struct sockaddr *)&sib, timeout_ms)) {
        set_os_error("rdma_resolve_addr", errno);
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject *Cm_resolve_route(CmObject *self, PyObject *args) {
    CHECK_ID(self);
    int timeout_ms;
    if (!PyArg_ParseTuple(args, "i", &timeout_ms)) return NULL;
    if (rdma_resolve_route(self->id, timeout_ms)) {
        set_os_error("rdma_resolve_route", errno);
        return NULL;
    }
    Py_RETURN_NONE;
}

/* Creates PD, completion channel + CQ, the RC queue pair, and registers the arena. */
static PyObject *Cm_setup(CmObject *self, PyObject *args) {
    CHECK_ID(self);
    if (self->cq) {
        PyErr_SetString(PyExc_ValueError, "queue pair already set up");
        return NULL;
    }
    int sq_depth, rq_depth, max_inline;
    Py_buffer arena;
    if (!PyArg_ParseTuple(args, "iiiw*", &sq_depth, &rq_depth, &max_inline, &arena)) return NULL;
    if (sq_depth < 1 || rq_depth < 1 || arena.len < 1) {
        PyBuffer_Release(&arena);
        PyErr_SetString(PyExc_ValueError, "queue depths and arena must be positive");
        return NULL;
    }
    struct ibv_context *verbs = self->id->verbs;
    int rc;

    self->pd = ibv_alloc_pd(verbs);
    if (!self->pd) { set_os_error("ibv_alloc_pd", errno); goto fail; }
    self->comp = ibv_create_comp_channel(verbs);
    if (!self->comp) { set_os_error("ibv_create_comp_channel", errno); goto fail; }
    if (set_nonblock(self->comp->fd)) { set_os_error("fcntl", errno); goto fail; }
    self->cq = ibv_create_cq(verbs, sq_depth + rq_depth, NULL, self->comp, 0);
    if (!self->cq) { set_os_error("ibv_create_cq", errno); goto fail; }

    struct ibv_qp_init_attr qa;
    memset(&qa, 0, sizeof(qa));
    qa.qp_type = IBV_QPT_RC;
    qa.send_cq = self->cq;
    qa.recv_cq = self->cq;
    qa.cap.max_send_wr = (uint32_t)sq_depth;
    qa.cap.max_recv_wr = (uint32_t)rq_depth;
    qa.cap.max_send_sge = 1;
    qa.cap.max_recv_sge = 1;
    qa.cap.max_inline_data = (uint32_t)max_inline;
    qa.sq_sig_all = 0;
    if (rdma_create_qp(self->id, self->pd, &qa)) { set_os_error("rdma_create_qp", errno); goto fail; }
    self->max_inline = qa.cap.max_inline_data;

    self->mr = ibv_reg_mr(self->pd, arena.buf, (size_t)arena.len, IBV_ACCESS_LOCAL_WRITE);
    if (!self->mr) { set_os_error("ibv_reg_mr", errno); goto fail; }

    rc = ibv_req_notify_cq(self->cq, 0);
    if (rc) { set_os_error("ibv_req_notify_cq", rc); goto fail; }

    self->view = arena;
    self->has_view = 1;
    Py_RETURN_NONE;

fail:
    PyBuffer_Release(&arena);
    return NULL;
}

static PyObject *Cm_connect_common(CmObject *self, PyObject *args, int accept) {
    CHECK_ID(self);
    Py_buffer priv;
    if (!PyArg_ParseTuple(args, "y*", &priv)) return NULL;
    if (priv.len > CM_MAX_PRIVATE) {
        PyBuffer_Release(&priv);
        PyErr_SetString(PyExc_ValueError, "private data too large");
        return NULL;
    }
    struct rdma_conn_param cp;
    memset(&cp, 0, sizeof(cp));
    cp.private_data = priv.buf;
    cp.private_data_len = (uint8_t)priv.len;
    cp.responder_resources = 1;
    cp.initiator_depth = 1;
    cp.retry_count = 7;
    cp.rnr_retry_count = 7; /* 7 means retry forever: a busy receiver delays, never fails */
    int rc = accept ? rdma_accept(self->id, &cp) : rdma_connect(self->id, &cp);
    int err = errno;
    PyBuffer_Release(&priv);
    if (rc) {
        set_os_error(accept ? "rdma_accept" : "rdma_connect", err);
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject *Cm_connect(CmObject *self, PyObject *args) { return Cm_connect_common(self, args, 0); }
static PyObject *Cm_accept(CmObject *self, PyObject *args) { return Cm_connect_common(self, args, 1); }

static PyObject *Cm_disconnect(CmObject *self, PyObject *Py_UNUSED(ignored)) {
    CHECK_ID(self);
    if (rdma_disconnect(self->id)) {
        set_os_error("rdma_disconnect", errno);
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject *addr_pair(const struct sockaddr_storage *ss) {
    if (ss->ss_family != AF_IB) Py_RETURN_NONE;
    const struct sockaddr_ib *sib = (const struct sockaddr_ib *)ss;
    return Py_BuildValue("(y#K)", (const char *)&sib->sib_addr, (Py_ssize_t)GID_LEN,
                         (unsigned long long)be64toh(sib->sib_sid));
}

static PyObject *Cm_peer(CmObject *self, PyObject *Py_UNUSED(ignored)) {
    CHECK_ID(self);
    return addr_pair(&self->id->route.addr.dst_storage);
}

static PyObject *Cm_local(CmObject *self, PyObject *Py_UNUSED(ignored)) {
    CHECK_ID(self);
    return addr_pair(&self->id->route.addr.src_storage);
}

static PyObject *Cm_post_recv(CmObject *self, PyObject *args) {
    CHECK_CQ(self);
    unsigned long long wr_id;
    Py_ssize_t offset, length;
    if (!PyArg_ParseTuple(args, "Knn", &wr_id, &offset, &length)) return NULL;
    if (offset < 0 || length < 0 || offset + length > self->view.len) {
        PyErr_SetString(PyExc_ValueError, "receive range outside the arena");
        return NULL;
    }
    struct ibv_sge sge;
    sge.addr = (uintptr_t)((char *)self->view.buf + offset);
    sge.length = (uint32_t)length;
    sge.lkey = self->mr->lkey;
    struct ibv_recv_wr wr, *bad = NULL;
    memset(&wr, 0, sizeof(wr));
    wr.wr_id = wr_id;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    int rc = ibv_post_recv(self->id->qp, &wr, &bad);
    if (rc) {
        set_os_error("ibv_post_recv", rc);
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject *Cm_post_send(CmObject *self, PyObject *args) {
    CHECK_CQ(self);
    unsigned long long wr_id, imm;
    Py_ssize_t offset, length;
    if (!PyArg_ParseTuple(args, "KnnK", &wr_id, &offset, &length, &imm)) return NULL;
    if (imm > 0xFFFFFFFFULL) {
        PyErr_SetString(PyExc_ValueError, "immediate data must fit in 32 bits");
        return NULL;
    }
    if (offset < 0 || length < 0 || offset + length > self->view.len) {
        PyErr_SetString(PyExc_ValueError, "send range outside the arena");
        return NULL;
    }
    struct ibv_sge sge;
    sge.addr = (uintptr_t)((char *)self->view.buf + offset);
    sge.length = (uint32_t)length;
    sge.lkey = self->mr->lkey;
    struct ibv_send_wr wr, *bad = NULL;
    memset(&wr, 0, sizeof(wr));
    wr.wr_id = wr_id;
    wr.opcode = IBV_WR_SEND_WITH_IMM;
    wr.imm_data = htobe32((uint32_t)imm);
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    int rc = ibv_post_send(self->id->qp, &wr, &bad);
    if (rc) {
        set_os_error("ibv_post_send", rc);
        return NULL;
    }
    Py_RETURN_NONE;
}

/* Small messages: the device copies the payload at post time, so no arena slot is needed. */
static PyObject *Cm_post_send_inline(CmObject *self, PyObject *args) {
    CHECK_CQ(self);
    unsigned long long wr_id;
    Py_buffer data;
    unsigned long long imm;
    if (!PyArg_ParseTuple(args, "Ky*K", &wr_id, &data, &imm)) return NULL;
    if (imm > 0xFFFFFFFFULL) {
        PyBuffer_Release(&data);
        PyErr_SetString(PyExc_ValueError, "immediate data must fit in 32 bits");
        return NULL;
    }
    if (data.len > (Py_ssize_t)self->max_inline) {
        PyBuffer_Release(&data);
        PyErr_Format(PyExc_ValueError, "%zd bytes exceed max inline %u", data.len, self->max_inline);
        return NULL;
    }
    struct ibv_sge sge;
    sge.addr = (uintptr_t)data.buf;
    sge.length = (uint32_t)data.len;
    sge.lkey = 0;
    struct ibv_send_wr wr, *bad = NULL;
    memset(&wr, 0, sizeof(wr));
    wr.wr_id = wr_id;
    wr.opcode = IBV_WR_SEND_WITH_IMM;
    wr.imm_data = htobe32((uint32_t)imm);
    wr.send_flags = IBV_SEND_SIGNALED | IBV_SEND_INLINE;
    wr.sg_list = data.len ? &sge : NULL;
    wr.num_sge = data.len ? 1 : 0;
    int rc = ibv_post_send(self->id->qp, &wr, &bad);
    PyBuffer_Release(&data);
    if (rc) {
        set_os_error("ibv_post_send", rc);
        return NULL;
    }
    Py_RETURN_NONE;
}

/* Returns [(wr_id, status, opcode, byte_len, imm)]; imm is 0 when absent. */
static PyObject *Cm_poll(CmObject *self, PyObject *args) {
    CHECK_CQ(self);
    int max;
    if (!PyArg_ParseTuple(args, "i", &max)) return NULL;
    if (max < 1) max = 1;
    if (max > MAX_POLL) max = MAX_POLL;
    struct ibv_wc wc[MAX_POLL];
    int got = ibv_poll_cq(self->cq, max, wc);
    if (got < 0) {
        set_os_error("ibv_poll_cq", -got);
        return NULL;
    }
    PyObject *out = PyList_New(got);
    if (!out) return NULL;
    for (int i = 0; i < got; i++) {
        uint32_t imm = (wc[i].wc_flags & IBV_WC_WITH_IMM) ? be32toh((uint32_t)wc[i].imm_data) : 0;
        PyObject *t = Py_BuildValue("(KiiII)", (unsigned long long)wc[i].wr_id, (int)wc[i].status,
                                    (int)wc[i].opcode, (unsigned)wc[i].byte_len, (unsigned)imm);
        if (!t) {
            Py_DECREF(out);
            return NULL;
        }
        PyList_SET_ITEM(out, i, t);
    }
    return out;
}

static PyObject *Cm_cq_fileno(CmObject *self, PyObject *Py_UNUSED(ignored)) {
    if (!self->comp) {
        PyErr_SetString(PyExc_ValueError, "queue pair not setup");
        return NULL;
    }
    return PyLong_FromLong(self->comp->fd);
}

/* Consumes pending completion-channel notifications (must precede polling after a wakeup). */
static PyObject *Cm_cq_drain(CmObject *self, PyObject *Py_UNUSED(ignored)) {
    CHECK_CQ(self);
    long count = 0;
    for (;;) {
        struct ibv_cq *ev_cq = NULL;
        void *ev_ctx = NULL;
        if (ibv_get_cq_event(self->comp, &ev_cq, &ev_ctx)) {
            if (errno == EAGAIN) break;
            set_os_error("ibv_get_cq_event", errno);
            return NULL;
        }
        ibv_ack_cq_events(ev_cq, 1);
        count++;
    }
    return PyLong_FromLong(count);
}

static PyObject *Cm_cq_arm(CmObject *self, PyObject *Py_UNUSED(ignored)) {
    CHECK_CQ(self);
    int rc = ibv_req_notify_cq(self->cq, 0);
    if (rc) {
        set_os_error("ibv_req_notify_cq", rc);
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject *Cm_close(CmObject *self, PyObject *Py_UNUSED(ignored)) {
    teardown(self);
    Py_RETURN_NONE;
}

static PyObject *Cm_get_max_inline(CmObject *self, void *Py_UNUSED(closure)) {
    return PyLong_FromUnsignedLong(self->max_inline);
}

static PyMethodDef Cm_methods[] = {
    {"fileno", (PyCFunction)Cm_fileno, METH_NOARGS, "rdma_cm event channel fd"},
    {"cm_event", (PyCFunction)Cm_cm_event, METH_NOARGS, "next rdma_cm event or None"},
    {"bind", (PyCFunction)Cm_bind, METH_VARARGS, "bind(gid16, service_id)"},
    {"listen", (PyCFunction)Cm_listen, METH_VARARGS, "listen(backlog)"},
    {"resolve", (PyCFunction)Cm_resolve, METH_VARARGS, "resolve(gid16, service_id, timeout_ms)"},
    {"resolve_route", (PyCFunction)Cm_resolve_route, METH_VARARGS, "resolve_route(timeout_ms)"},
    {"setup", (PyCFunction)Cm_setup, METH_VARARGS, "setup(sq_depth, rq_depth, max_inline, arena)"},
    {"connect", (PyCFunction)Cm_connect, METH_VARARGS, "connect(private_data)"},
    {"accept", (PyCFunction)Cm_accept, METH_VARARGS, "accept(private_data)"},
    {"disconnect", (PyCFunction)Cm_disconnect, METH_NOARGS, "rdma_disconnect"},
    {"peer", (PyCFunction)Cm_peer, METH_NOARGS, "(gid, service_id) of the peer or None"},
    {"local", (PyCFunction)Cm_local, METH_NOARGS, "(gid, service_id) of the local side or None"},
    {"post_recv", (PyCFunction)Cm_post_recv, METH_VARARGS, "post_recv(wr_id, offset, length)"},
    {"post_send", (PyCFunction)Cm_post_send, METH_VARARGS, "post_send(wr_id, offset, length, imm)"},
    {"post_send_inline", (PyCFunction)Cm_post_send_inline, METH_VARARGS, "post_send_inline(wr_id, data, imm)"},
    {"poll", (PyCFunction)Cm_poll, METH_VARARGS, "poll(max) -> completions"},
    {"cq_fileno", (PyCFunction)Cm_cq_fileno, METH_NOARGS, "completion channel fd"},
    {"cq_drain", (PyCFunction)Cm_cq_drain, METH_NOARGS, "consume completion notifications"},
    {"cq_arm", (PyCFunction)Cm_cq_arm, METH_NOARGS, "re-arm the completion notification"},
    {"close", (PyCFunction)Cm_close, METH_NOARGS, "release all verbs and cm resources"},
    {NULL, NULL, 0, NULL}};

static PyGetSetDef Cm_getset[] = {
    {"max_inline", (getter)Cm_get_max_inline, NULL, "negotiated max inline bytes", NULL},
    {NULL, NULL, NULL, NULL, NULL}};

static PyTypeObject CmType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "pureib._verbs.Cm",
    .tp_basicsize = sizeof(CmObject),
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = "One rdma_cm connection (listener or peer) with its queue pair and arena.",
    .tp_new = PyType_GenericNew,
    .tp_init = (initproc)Cm_init,
    .tp_dealloc = (destructor)Cm_dealloc,
    .tp_methods = Cm_methods,
    .tp_getset = Cm_getset,
};

/* Local IB ports that are ACTIVE, as (device_name, gid16, port) tuples. */
static PyObject *mod_local_addresses(PyObject *Py_UNUSED(self), PyObject *Py_UNUSED(ignored)) {
    PyObject *out = PyList_New(0);
    if (!out) return NULL;
    int n = 0;
    struct ibv_device **devs = ibv_get_device_list(&n);
    if (!devs) return out; /* no verbs stack or no devices */
    for (int i = 0; i < n; i++) {
        struct ibv_context *ctx = ibv_open_device(devs[i]);
        if (!ctx) continue;
        struct ibv_device_attr da;
        if (ibv_query_device(ctx, &da) == 0) {
            for (uint8_t port = 1; port <= da.phys_port_cnt; port++) {
                struct ibv_port_attr pa;
                if (ibv_query_port(ctx, port, &pa)) continue;
                if (pa.state != IBV_PORT_ACTIVE || pa.link_layer != IBV_LINK_LAYER_INFINIBAND) continue;
                union ibv_gid gid;
                if (ibv_query_gid(ctx, port, 0, &gid)) continue;
                PyObject *t = Py_BuildValue("(sy#i)", ibv_get_device_name(devs[i]), (const char *)&gid,
                                            (Py_ssize_t)GID_LEN, (int)port);
                if (!t || PyList_Append(out, t)) {
                    Py_XDECREF(t);
                    ibv_close_device(ctx);
                    ibv_free_device_list(devs);
                    Py_DECREF(out);
                    return NULL;
                }
                Py_DECREF(t);
            }
        }
        ibv_close_device(ctx);
    }
    ibv_free_device_list(devs);
    return out;
}

static PyMethodDef module_methods[] = {
    {"local_addresses", mod_local_addresses, METH_NOARGS, "ACTIVE native IB ports and their GID 0"},
    {NULL, NULL, 0, NULL}};

static struct PyModuleDef verbs_module = {
    .m_base = PyModuleDef_HEAD_INIT,
    .m_name = "_verbs",
    .m_doc = "Native InfiniBand verbs and rdma_cm binding for pureib.",
    .m_size = -1,
    .m_methods = module_methods,
};

PyMODINIT_FUNC PyInit__verbs(void) {
    if (PyType_Ready(&CmType) < 0) return NULL;
    PyObject *m = PyModule_Create(&verbs_module);
    if (!m) return NULL;
    if (PyModule_AddIntConstant(m, "ABI_VERSION", 1) < 0 ||
        PyModule_AddObjectRef(m, "Cm", (PyObject *)&CmType) < 0) {
        Py_DECREF(m);
        return NULL;
    }
    return m;
}
