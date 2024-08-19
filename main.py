from part_1.exec_1 import Exec1 as p1_exec_1
from part_1.exec_2 import Exec2 as p1_exec_2
from part_1.exec_3 import Exec3 as p1_exec_3

from part_2.exec_1 import Exec1 as p2_exec_1
from part_2.exec_2 import Exec2 as p2_exec_2

from part_3.exec_1 import Exec1 as p3_exec_1
from part_3.exec_2 import Exec2 as p3_exec_2

from part_4.exec_1 import Exec1 as p4_exec_1

from part_5.action_log import ActionLog as p5_action_log

import logging
import sys

logger = logging.getLogger(__name__)

def start_logger():
    logging.basicConfig(level=logging.INFO,     
                        format="%(asctime)s [%(levelname)s] %(message)s",
                        handlers=[
                            logging.FileHandler("./logs/debug.log"),
                            logging.StreamHandler(sys.stdout)
                        ])


if __name__ == "__main__":
    start_logger()
    logger.info("Process Started")
    p1_exec_1().execute()
    p1_exec_2().execute()
    p1_exec_3().execute()

    p2_exec_1().execute()
    p2_exec_2().execute()

    p3_exec_1().execute()
    p3_exec_2().execute()

    p4_exec_1().execute()

    p5_action_log().execute()

    logger.info("Process Finished")
