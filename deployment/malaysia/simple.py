from deployment.core import Master
from publisher.core import Boundary


class SimpleKlangValleyDeploymentMaster(Master):
    def __init__(self):
        super(SimpleKlangValleyDeploymentMaster, self).__init__()
        self.boundary = Boundary(100.711638, 3.870733, 101.970674, 2.533530)

    def decide(self, payload):
        pass


if __name__ == "__main__":
    m = SimpleKlangValleyDeploymentMaster()
    m.start()
