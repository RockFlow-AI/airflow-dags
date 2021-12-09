import unittest
from datetime import datetime

from rockflow.common.datatime_helper import GmtDatetimeCheck, GMT_FORMAT


class Test(unittest.TestCase):
    def test(self):
        assert GmtDatetimeCheck("Thu, 08 Dec 2020 07:39:31 GMT", weeks=1).check == True
        assert GmtDatetimeCheck(datetime.now().strftime(GMT_FORMAT), weeks=1).check == False


if __name__ == '__main__':
    unittest.main()
