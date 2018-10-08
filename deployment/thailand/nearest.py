from base import setup_logging, Boundary, PlayerInstruction, deserialize_message, DeferDecision
from deployment.core import DeploymentMaster
from gamemaster.core import Status
from gamemaster.vehicle import ask_osrm


class NearestAssessorBangkokDeploymentMaster(DeploymentMaster):
    def __init__(self):
        binding_keys = ['thailand.greater_bangkok']
        super(NearestAssessorBangkokDeploymentMaster, self).__init__(binding_keys)
        self.logger = setup_logging('NearestAssessorBangkokDeploymentMaster')
        self.boundary = Boundary(13.381014, 100.950397, 14.284958, 99.869143)

    def decide(self, payload):
        def get_status(index):
            rpc = self.ask(index, PlayerInstruction.STATUS)
            return deserialize_message(rpc.body)

        def is_unavailable(player):
            self.logger.debug('checking player=%r', player)
            status = Status(player['status'])
            return status is not Status.EN_ROUTE or status is not Status.ASSESSING

        player_statuses = map(get_status, range(self.player_count))
        possible_player = list(filter(is_unavailable, player_statuses))
        if len(possible_player) <= 0:
            raise DeferDecision

        dest = payload.location.to_dict()
        osrm_timing = dict(map(lambda p: (ask_osrm(p['destination'], dest)['routes'][0]['duration'], p), possible_player))
        self.logger.debug('decide using=%r', osrm_timing)
        player = osrm_timing[min(osrm_timing)]
        self.deploy(player['player_id'], payload)


if __name__ == "__main__":
    m = NearestAssessorBangkokDeploymentMaster()
    m.start()
