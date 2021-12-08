import json

from bs4 import BeautifulSoup

from rockflow.common.downloader import Downloader


class FutuCompanyProfile(Downloader):
    def __init__(self, symbol, futu_ticker, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.symbol = symbol
        self.futu_ticker = futu_ticker

    @property
    def language(self):
        raise NotImplementedError()

    @property
    def url(self):
        raise NotImplementedError()

    @property
    def type(self):
        return "html"

    def raw_data(self, fp):
        soup = BeautifulSoup(fp, features="lxml")
        table_dict = {}
        for row in soup.findAll(name="div", attrs={"class": "company-item"}):
            try:
                name = row.find('div', class_='name').find(text=True).strip()
            except:
                continue
            try:
                value = row.find('div', class_='value').find(text=True).strip()
            except:
                continue
            table_dict[name] = value
        table_dict["symbol"] = self.symbol
        table_dict["futu_ticker"] = self.futu_ticker
        table_dict["language"] = self.language
        return table_dict

    def format(self, table_dict):
        raise NotImplementedError()

    def to_json(self, fp):
        raw_table = self.raw_data(fp)
        # print(json.dumps(raw_table, ensure_ascii=False))
        table_dict = self.format(raw_table)
        print(json.dumps(table_dict, ensure_ascii=False))
        return table_dict


class FutuCompanyProfileEn(FutuCompanyProfile):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def language(self):
        return "en"

    @property
    def url(self):
        return f"https://www.futunn.com/{self.language}/stock/{self.futu_ticker}/company-profile"

    def format(self, table_dict):
        new_table = {}
        new_table["symbol"] = table_dict.get("symbol")
        new_table["futu_ticker"] = table_dict.get("futu_ticker")
        new_table["language"] = table_dict.get("language")
        new_table["market"] = table_dict.get("market")

        new_table["short_name_a"] = table_dict.get("Short name-A")
        new_table["short_name_h"] = table_dict.get("Short name-H")
        new_table["name"] = table_dict.get("Company Name")
        new_table["profile"] = table_dict.get("Profile")
        new_table["business"] = table_dict.get("Business")
        new_table["exchange"] = table_dict.get("Market") if table_dict.get("Market") else table_dict.get(
            "Listed exchange")
        return new_table


# class FutuCompanyProfileHk(FutuCompanyProfile):
#     def __init__(self, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#
#     @property
#     def language(self):
#         return "hk"
#
#     @property
#     def url(self):
#         return f"https://www.futunn.com/{self.language}/stock/{self.futu_ticker}/company-profile"


class FutuCompanyProfileCn(FutuCompanyProfile):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def language(self):
        return "cn"

    @property
    def url(self):
        return f"https://www.futunn.com/stock/{self.futu_ticker}/company-profile"

    def format(self, table_dict):
        new_table = {}
        new_table["symbol"] = table_dict.get("symbol")
        new_table["futu_ticker"] = table_dict.get("futu_ticker")
        new_table["language"] = table_dict.get("language")
        new_table["market"] = table_dict.get("market")

        new_table["short_name_a"] = table_dict.get("A股证券简称")
        new_table["short_name_h"] = table_dict.get("H股证券简称")
        new_table["name"] = table_dict.get("公司名称")
        new_table["profile"] = table_dict.get("公司简介")
        new_table["business"] = table_dict.get("公司业务") if table_dict.get("公司业务") else table_dict.get("公司主营")
        new_table["exchange"] = table_dict.get("所属市场") if table_dict.get("所属市场") else table_dict.get("上市交易所")
        return new_table
