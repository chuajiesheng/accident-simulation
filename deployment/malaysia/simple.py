import random

from base import setup_logging, Boundary, PlayerInstruction
from deployment.core import DeploymentMaster


class SimpleKlangValleyDeploymentMaster(DeploymentMaster):
    def __init__(self):
        super(SimpleKlangValleyDeploymentMaster, self).__init__()
        self.logger = setup_logging('SimpleKlangValleyDeploymentMaster')
        self.boundary = Boundary(100.711638, 3.870733, 101.970674, 2.533530)

    def decide(self, payload):
        player = random.randrange(self.player_count)
        self.tell(player, PlayerInstruction.GO, payload)


if __name__ == "__main__":
    m = SimpleKlangValleyDeploymentMaster()
    m.start()
