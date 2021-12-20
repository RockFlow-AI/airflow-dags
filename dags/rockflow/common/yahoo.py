import os

import httpx

from rockflow.common.downloader import Downloader

modules = [
    "assetProfile",
    "balanceSheetHistory",
    "balanceSheetHistoryQuarterly",
    "calendarEvents",
    "cashflowStatementHistory",
    "cashflowStatementHistoryQuarterly",
    "defaultKeyStatistics",
    "earnings",
    "earningsHistory",
    "earningsTrend",
    "financialData",
    "fundOwnership",
    "incomeStatementHistory",
    "incomeStatementHistoryQuarterly",
    "indexTrend",
    "industryTrend",
    "insiderHolders",
    "insiderTransactions",
    "institutionOwnership",
    "majorDirectHolders",
    "majorHoldersBreakdown",
    "netSharePurchaseActivity",
    "price",
    "quoteType",
    "recommendationTrend",
    "secFilings",
    "sectorTrend",
    "summaryDetail",
    "summaryProfile",
    "symbol",
    "upgradeDowngradeHistory",
    "fundProfile",
    "topHoldings",
    "fundPerformance",
]


class Yahoo(Downloader):
    def __init__(self, symbol, prefix: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.symbol = symbol
        self.prefix = prefix

    @property
    def url(self):
        return f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{self.symbol}"

    @property
    def params(self):
        return {
            "modules": ",".join(modules),
        }

    @property
    def type(self):
        return "json"

    @property
    def oss_key(self):
        return os.path.join(
            f"{self.prefix}_{self.lowercase_class_name}",
            f"{self.symbol}.{self.type}",
        )

    def check(self, r: httpx.Response) -> bool:
        if not super().check(r):
            return False
        r_json = r.json()
        return "quoteSummary" not in r_json or \
               "result" not in r_json["quoteSummary"] or \
               not r_json["quoteSummary"]["result"] or \
               ("error" not in r_json["quoteSummary"]
                or not r_json["quoteSummary"]["error"])
