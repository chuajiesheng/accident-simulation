import random

from base import setup_logging
from deployment.core import DeploymentMaster


class SimpleKlangValleyDeploymentMaster(DeploymentMaster):
    def __init__(self):
        super(SimpleKlangValleyDeploymentMaster, self).__init__()
        self.logger = setup_logging('SimpleKlangValleyDeploymentMaster')


    def decide(self, payload):
        player = random.randrange(self.player_count)
        self.tell(player, DeploymentMaster.Action.GO, payload)


if __name__ == "__main__":
    m = SimpleKlangValleyDeploymentMaster()
    m.start()
