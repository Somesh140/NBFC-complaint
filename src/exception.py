import os,sys


class CustomException(Exception):
    def __init__(self, error_message:Exception,error_detail:sys):
        super.__init__(error_message)
        self.error_message=error_message
    
    @staticmethod
    def get_detailed_error_message(error_message:Exception(),error_detail:sys)->str:
        """
        error_message: Exception object
        error_detail: object of sys module
        """
        _,_,exec_tb=error_detail.exc_info()
        line = exec_tb.tb_lineno
        file_name = exec_tb.tb_frame.f_code.co_filename
        error_message="""Error occurred in file: [{file_name}] at line number: [{line}] 
            message:[{error_message}]""".format(file_name=file_name,line=line,error_message=error_message)
        
        return error_message

    def __str__(self):
        return self.error_message

    def __repr__(self)->str:
        return CustomException.__name__.str()
