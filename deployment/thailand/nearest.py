import random

from base import setup_logging, Boundary, PlayerInstruction, deserialize_message
from deployment.core import DeploymentMaster
from gamemaster.vehicle import ask_osrm


class NearestAssessorBangkokDeploymentMaster(DeploymentMaster):
    def __init__(self):
        binding_keys = ['thailand.greater_bangkok']
        super(NearestAssessorBangkokDeploymentMaster, self).__init__(binding_keys)
        self.logger = setup_logging('NearestAssessorBangkokDeploymentMaster')
        self.boundary = Boundary(13.381014, 100.950397, 14.284958, 99.869143)

    def decide(self, payload):
        time_to_dest = dict()
        for i in range(self.player_count):
            rpc = self.ask(i, PlayerInstruction.DESTINATION)
            player_dest = deserialize_message(rpc.body)['player']
            time_to_dest[i] = ask_osrm(player_dest, payload.location.to_dict())['routes'][0]['duration']

        self.logger.debug('decide using=%r', time_to_dest)
        player = min(time_to_dest, key=time_to_dest.get)
        self.deploy(player, payload)


if __name__ == "__main__":
    m = NearestAssessorBangkokDeploymentMaster()
    m.start()
