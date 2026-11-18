from setuptools import Extension, setup

setup(
    ext_modules=[
        Extension(
            'pureib._verbs',
            sources=['csrc/_verbs.c'],
            libraries=['ibverbs', 'rdmacm'],
            extra_compile_args=['-Wall', '-Wextra', '-Werror', '-Wno-unused-parameter'],
        )
    ]
)
