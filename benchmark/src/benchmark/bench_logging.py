
import logging


def setup_logger():
    formatter = logging.Formatter('%(asctime)s.%(msecs)03d %(levelname)s %(message)s',
                                  '%Y-%m-%d %H:%M:%S')
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    root = logging.getLogger()
    hdlr = root.handlers[0]
    hdlr.setFormatter(formatter)



