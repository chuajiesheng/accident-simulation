import random

from deployment.core import Master
from publisher.core import Boundary


class SimpleKlangValleyDeploymentMaster(Master):
    def __init__(self):
        super(SimpleKlangValleyDeploymentMaster, self).__init__()

    def decide(self, payload):
        player = random.randrange(self.player_count)
        self.tell(player, Master.Action.GO, payload)


if __name__ == "__main__":
    m = SimpleKlangValleyDeploymentMaster()
    m.start()
