from multiprocessing import Manager

''' Shared object betweem processss '''


class Shared:

    def __init__(self):
        self.dict = Manager().dict()

    def write(self, index, msg):
        self.dict[index] = msg


shared = Shared()
shared.write("meta", False)
