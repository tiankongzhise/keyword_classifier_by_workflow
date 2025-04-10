from typing import Any

class Message(object):

    _info_map = {"DEBUG":1,"INFO":2,"WARNING":3,"ERROR":4,"CRITICAL":5}
    def __init__(self,*args,**kwargs):
        self.args = args
        self.kwargs = kwargs
        self.level = 'INFO'
        self.define_handler = self.set_handler()
    def __call__(self,*args,**kwargs):
        print(*args,**kwargs)

    def set_handler(self,handler:Any=None):
        if handler:
            self.define_handler = handler
        
        return self._print
            
        

    def _print(self,*args,**kwargs):
        print(*args,**kwargs)
    
    def debug(self,*args,**kwargs):
        if self._info_map[self.level] <= self._info_map['DEBUG']:
            self.define_handler(*args,**kwargs)

    def info(self,*args,**kwargs):
        if self._info_map[self.level] <= self._info_map['INFO']:
            self.define_handler(*args,**kwargs)
    
    def warning(self,*args,**kwargs):
        if self._info_map[self.level] <= self._info_map['WARNING']:
            self.define_handler(*args,**kwargs)
    
    def error(self,*args,**kwargs):
        if self._info_map[self.level] <= self._info_map['ERROR']:
            self.define_handler(*args,**kwargs)
    
    def critical(self,*args,**kwargs):
        if self._info_map[self.level] <= self._info_map['CRITICAL']:
            self.define_handler(*args,**kwargs)



message = Message()
