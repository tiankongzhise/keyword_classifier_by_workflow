

class Message(object):
    def __init__(self,*args,**kwargs):
        self.args = args
        self.kwargs = kwargs
        self.define_handler = self.set_handler()
    def __call__(self,*args,**kwargs):
        print(*args,**kwargs)

    def set_handler(self,handler:str='print'):
        if handler == 'print':
            return self._print
        
        return self._print
            
        

    def _print(self,*args,**kwargs):
        print(*args,**kwargs)
    
    def debug(self,*args,**kwargs):
        self.define_handler(*args,**kwargs)

    def info(self,*args,**kwargs):
        self.define_handler(*args,**kwargs)
    
    def warning(self,*args,**kwargs):
        self.define_handler(*args,**kwargs)
    
    def error(self,*args,**kwargs):
        self.define_handler(*args,**kwargs)
    
    def critical(self,*args,**kwargs):
        self.define_handler(*args,**kwargs)



message = Message()
