from rockflow.common.apollo_symbol_downloader import ApolloUS


class ApolloNasdaq(ApolloUS):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def market(self):
        return 'nasdaq'
