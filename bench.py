import time

def test():
    """Stupid test function"""
    for x in range(10):
        return time.monotonic()

if __name__ == '__main__':
    import timeit

    # For Python>=3.5 one can also write:
    print(timeit.timeit("time.monotonic()", globals=locals()))
