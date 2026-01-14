from swarm.models.capacities import Capacities
from swarm.models.role import Role
from swarm.comm.colmena_consensus import colmena_consensus_pb2_grpc, colmena_consensus_pb2
from google.protobuf import empty_pb2
import logging

LOG = logging.getLogger(__name__)


class SelectionServicer(colmena_consensus_pb2_grpc.SelectionServiceServicer):
    def __init__(self, trigger_consensus_fn):
        """
        trigger_consensus_fn: function(job_list) -> triggers consensus for given jobs
        """
        self.trigger_consensus_fn = trigger_consensus_fn

    def RequestRoles(self, request, context):
        LOG.info("Received RequestRoles for role=%s, service=%s", request.role.roleId, request.role.serviceId)

        # Wrap the request in your internal Job structure if needed
        role = Role()
        role.role_id = request.role.roleId
        role.service_id = request.role.serviceId
        role.startOrStop = request.startOrStop

        #job.set_min_or_max(request.startOrStop)
        resources = {res.name: res.value for res in request.role.resources}
        role.set_capacities(Capacities.from_dict(resources))

        # Call your agent's trigger_consensus
        self.trigger_consensus_fn(role)

        return empty_pb2.Empty()
