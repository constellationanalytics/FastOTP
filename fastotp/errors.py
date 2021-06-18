import traceback


def log_error(e, logger=None):
    traceback = e.__traceback__
    msg = str(e)
    while traceback:
        msg += "\n{}: {}".format(traceback.tb_frame.f_code.co_filename,traceback.tb_lineno)
        traceback = traceback.tb_next
    if logger is not None:
        logger.warning(msg)
    else:
        print(msg)