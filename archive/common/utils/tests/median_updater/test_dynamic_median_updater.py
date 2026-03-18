from archive.common.utils.tests.median_updater.harness import test_median_finder
from common.utils.dynamic_median_updater import DynamicMedianUpdater

def _test():
    test_median_finder(DynamicMedianUpdater(), seed=43972)

if __name__ == "__main__":
    _test()