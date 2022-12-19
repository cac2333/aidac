class ForceLocalExecutionWarning(UserWarning):
    def __str__(self):
        return f'ForceLocalExecutionWarning: ' \
               f'the operation is not supported for remote execution, thus local execution will be forced'
