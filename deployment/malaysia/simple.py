import random

from base import setup_logging, Boundary, PlayerInstruction
from deployment.core import DeploymentMaster


class SimpleKlangValleyDeploymentMaster(DeploymentMaster):
    def __init__(self):
        binding_keys = ['malaysia.klang_valley']
        super(SimpleKlangValleyDeploymentMaster, self).__init__(binding_keys)
        self.logger = setup_logging('SimpleKlangValleyDeploymentMaster')
        self.boundary = Boundary(3.398051, 101.433717, 2.776582, 101.995776)

    def decide(self, payload):
        player = random.randrange(self.player_count)
        self.deploy(player, payload)


if __name__ == "__main__":
    m = SimpleKlangValleyDeploymentMaster()
    m.start()
