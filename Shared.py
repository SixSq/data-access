from multiprocessing import Manager

''' Shared object between processes '''


class Shared:

    def __init__(self):
        self.dict = Manager().dict()

    def write(self, k, v):
        self.dict[k] = v


shared = Shared()
shared.write("meta", False)
