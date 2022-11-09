from rockflow.common.apollo_symbol_downloader import ApolloUS
from rockflow.operators.const import APOLLO_HOST, APOLLO_PORT


class ApolloOTC(ApolloUS):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def market(self):
        return 'otc'

    @property
    def url(self):
        return f"http://{APOLLO_HOST}:{APOLLO_PORT}/configs/flow-ticker-service/PROD/application"
