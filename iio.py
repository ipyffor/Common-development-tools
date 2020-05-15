import random
# import sys
# sys.setrecursionlimit(2000000)
class IDataSet:

    def __init__(self):
        self.reader = None
    
    def __call__(self):
        return self.reader
    
    def get_reader(zip_data):
        def reader():
            for d in zip_data:
                yield d
        iset = IDataSet()
        iset.reader = reader
        return iset
    def shuffle(self, buf_size):
        r = self.reader
        def data_reader():
            buf = []
            for e in r():
                buf.append(e)
                if len(buf) >= buf_size:
                    random.shuffle(buf)
                    # print(buf)
                    for b in buf:
                        # print('..', b)
                        yield b
                    buf = []

            if len(buf) > 0:
                random.shuffle(buf)
                for b in buf:
                    yield b
        self.reader = data_reader
        return self
    def batch(self, batch_size, drop_last=False):
        
        r = self.reader()##这条语句切记在定义函数之前执行，因为函数定义不会执行，等到调用的时候用的永远是最新的self.reader，而不是定义函数时期望的,会产生无穷的递归
        def batch_reader():
            b = []
            
            for i, instance in enumerate(r):
                if i == 0:
                     d_len = len(instance)
                     for i in range(d_len):
                        b.append([])
                for id, item in enumerate(instance):
                    b[id].append(item)
                if len(b[0]) == batch_size:
                    yield b
                    for i in range(d_len):
                        b[i] = []
            if drop_last == False and len(b[0]) != 0:
                yield b

        # Batch size check
        batch_size = int(batch_size)
        if batch_size <= 0:
            raise ValueError("batch_size should be a positive integeral value, "
                            "but got batch_size={}".format(batch_size))
        self.reader = batch_reader
        return self