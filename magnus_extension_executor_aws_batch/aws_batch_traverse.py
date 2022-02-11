import logging
from random import randint
import re
from time import sleep


from magnus.executor import BaseExecutor
from magnus.graph import search_node_by_internal_name
from magnus.nodes import BaseNode
from magnus.datastore import StepLog
from magnus import utils
from magnus import defaults


logger = logging.getLogger(defaults.NAME)

try:
    from magnus_extension_aws_config.aws import AWSConfigMixin
except ImportError as _e:
    msg = (
        'Please install magnus_extension_aws_config which provides the general utilities for AWS services.'
    )
    raise Exception(msg) from _e


class LocalAWSBatchExecutor(BaseExecutor, AWSConfigMixin):
    """
    This mode should help in off-loading jobs to AWS Batch.

    The mode should self traverse the graph and call the next node accordingly.

    Example config:

    mode:
      type: local-aws-batch
      config:
        aws_batch_job_definition: The batch job definition to use
        aws_batch_job_queue: The batch job queue to send the jobs
        use_profile_in_batch_job: defaults to False as batch jobs use role based access not profile
        region: The aws region to use
        tags:
           Any optional tags that you want to use in the batch job
        aws_profile: The aws profile to use to submit the batch jobs.


    """
    service_name = 'local-aws-batch'

    def __init__(self, config):
        # pylint: disable=R0914,R0913
        super().__init__(config=config)

    @property
    def tags(self) -> dict:
        """
        Provides a mechanism to have tags attached to compute.

        Returns:
            dict: The dictionary of tags
        """
        tags = None
        if 'tags' in self.config:
            tags = self.config['tags']
        return tags

    def is_parallel_execution(self) -> bool:  # pylint: disable=R0201
        """
        Controls the parallelization of branches in map and parallel state.

        We do not allow parallel execution to happen with batch within a single job.

        Returns:
            bool: False as within a single job we do not allow parallelization
        """
        return False

    def prepare_for_node_execution(self, node: BaseNode, map_variable: dict = None):
        """
        This method would be called prior to execute_node.
        Perform any steps required before doing the node execution.

        Check implementations in local, local-container, local-aws-batch for examples of implementations
        Args:
            node (Node): The node to execute
            map_variable (dict, optional): If the node is of a map state, map_variable is the value of the iterable.
                        Defaults to None.
        """
        remove_aws_profile = True

        if 'use_profile_in_batch_job' in self.config and self.config['use_profile_in_batch_job'].lower() == 'true':
            remove_aws_profile = False

        if remove_aws_profile:
            self.remove_aws_profile()

        super().prepare_for_node_execution(node=node, map_variable=map_variable)

    def execute_node(self, node, map_variable: dict = None, **kwargs):
        """
        We call super().execute_node to do the actual work.

        Check the step log for the status.

        If the node is an end node:
            If you are part of a branch, i.e node.internal_branch_name is not None:
                Check the branch log and see if all the branches are resolved
            If you are in main branch:
                We are done

        else:
            Get the next node, either by success or failure and self.execute_from_graph(node, **kwargs)
        """
        super().execute_node(node, map_variable=map_variable, **kwargs)

        dag_being_run = self.dag

        if node.node_type in ['success', 'fail']:
            # Case where the graph containing the node is simple and has no branches
            if node.internal_branch_name is None:
                logger.info('We reached the end of the graph!')
                return
            # Case where the graph containing the node has branches.
            logger.info(f'We have reached the end of the sub-graph: {node.internal_branch_name}')
            parent_step_name = '.'.join(node.internal_branch_name.split('.')[:-1])
            parent_step, _ = search_node_by_internal_name(dag=self.dag, internal_name=parent_step_name)
            parent_step_log = self.run_log_store.get_step_log(parent_step.internal_name, self.run_id)
            parent_step_log.status = defaults.SUCCESS

            sleep(randint(1, 5))  #  To avoid syncs, we wait for a random time between 1 and 5 seconds

            for _, branch in parent_step_log.branches.items():
                if branch.status == defaults.PROCESSING:
                    return
                if branch.status == defaults.FAIL:
                    parent_step_log.status = defaults.FAIL
                    break
            # Point the node to the step that contains the branches
            node = parent_step
            self.run_log_store.add_step_log(parent_step_log, self.run_id)

        if node.internal_branch_name:
            _, dag_being_run = search_node_by_internal_name(self.dag, node.internal_name)

        _, next_node_name = self.get_status_and_next_node_name(node, dag_being_run, map_variable=map_variable)
        next_node = dag_being_run.get_node_by_name(next_node_name)
        self.execute_from_graph(next_node, map_variable=map_variable)

    def trigger_job(self, node: BaseNode, map_variable: dict = None, **kwargs):
        """
        In this mode, we submit the batch job to execute magnus execute_single_node

        Args:
            node (BaseNode): The node we are currently executing
            map_variable (str, optional): If the node is part a map branch. Defaults to ''.
        """
        self._submit_batch_job(node, map_variable=map_variable, **kwargs)

    # TODO: map_variable should be part of the job name
    def add_code_identities(self, node: BaseNode, step_log: StepLog, **kwargs):
        """
        Call the base class to add the code based identity and then add ECR image sha

        Args:
            node (BaseNode): The node we are adding the code identity
            step_log (object): The step log of the corresponding node

        Raises:
            Exception: If 'aws_batch_job_definition' is not part of the config
            Exception: if 'aws_batch_job_queue' is not part of the config
        """
        super().add_code_identities(node, step_log)
        mode_config = {**node.get_mode_config(), **self.config}  #  Overrides node's mode config to global one

        if 'aws_batch_job_definition' not in mode_config:
            raise Exception(f'For mode type: {self.service_name}, \
                    config parameter "aws_batch_job_definition should be provided"')
        if 'aws_batch_job_queue' not in mode_config:
            raise Exception(f'For mode type: {self.service_name}, \
                    config parameter "aws_batch_job_queue should be provided"')

        # image_hash, image_from_job_definition = aws_utils.get_ecr_image_details_from_job_definition(
        #     job_definition_name=mode_config['aws_batch_job_definition'],
        #     boto_session=self.get_boto3_session())

        code_id = self.run_log_store.create_code_identity()
        code_id.code_identifier = None  #  Need to add this.
        code_id.code_identifier_type = 'ecr-docker'
        code_id.code_identifier_dependable = True
        code_id.code_identifier_url = None
        step_log.code_identities.append(code_id)

    def _submit_batch_job(self, node,  map_variable: dict = None, **kwargs):  # pylint: disable=unused-argument
        """"
        Submit the batch job
        """
        boto3_session = self.get_boto3_session()
        client = boto3_session.client('batch')

        command = utils.get_node_execution_command(self, node, map_variable=map_variable)
        logger.info(f'Triggering a batch job with {command}')

        job_name = re.sub('[^A-Za-z0-9]+', '-', f'{self.run_id}-{node.internal_name}')[:127]

        mode_config = {**node.get_mode_config(), **self.config}  #  Overrides node's mode config to global one
        if 'aws_batch_job_definition' not in mode_config:
            raise Exception('For mode type: local-aws-batch, \
                    config parameter "aws_batch_job_definition should be provided"')
        if 'aws_batch_job_queue' not in mode_config:
            raise Exception('For mode type: local-aws-batch, \
                    config parameter "aws_batch_job_queue should be provided"')

        if not self.tags:
            self.tags = {}

        response = client.submit_job(
            jobName=job_name,
            jobDefinition=mode_config['aws_batch_job_definition'],
            jobQueue=mode_config['aws_batch_job_queue'],
            tags=self.tags,
            retryStrategy={'attempts': 1},
            containerOverrides={'command': command.split()})
        logger.info(f'Triggered Batch job with {response}')

        step_log = self.run_log_store.get_step_log(node.get_step_log_name(map_variable), self.run_id)
        step_log.status = defaults.TRIGGERED
        self.run_log_store.add_step_log(step_log, self.run_id)
