from multiprocessing.pool import ThreadPool as Pool

DEFAULT_POOL_SIZE = 24
DEFAULT_POOL = Pool(processes=DEFAULT_POOL_SIZE)
