from rockflow.common.nasdaq import Nasdaq
from rockflow.common.hkex import HKEX
import unittest

r = Nasdaq()
# r = HKEX()
# print(r.get())
print(r.get().content)
# class NasdaqDownloadTest(unittest.TestCase):
#     def test_downloadable(self):
#         r=Nasdaq.get()
#         self.assertTrue(r)
#         self.fail(str(r))
