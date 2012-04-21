import types, marshal

def freeze_function(f):
    return marshal.dumps(f.func_code)
    
def gen_custom_import(allowed_modules):
    def custom_import(name, _globals=None, _locals=None, fromlist=None, level=-1):
        """Prevent the import of disallowed modules"""
        if fromlist is None:
            fromlist = []
        if _locals is None:
            _locals = {}
        if _globals is None:
            _globals = {}
        if allowed_modules is None or name in allowed_modules:
            return __import__(name, _globals, _locals, fromlist, level)
        else:
            raise Exception('Nuh uh Mr. Burrito man!')
    return custom_import

def unfreeze_and_sandbox_function(frozen_fn, name, allowed_modules=None):
    disallowed_builtins = ['eval', 'execfile', 'open', 'print', 'raw_input', 'reload']
    _globals = globals()
    _builtins = dict(_globals['__builtins__'])
    for prop in disallowed_builtins:
       del _builtins[prop]
    _builtins['__import__'] = gen_custom_import(allowed_modules)
    gs = {
        '__builtins__': _builtins,
        '__file__': None, '__name__': None, '__doc__': None,
    }
    return types.FunctionType(marshal.loads(frozen_fn), gs, name)