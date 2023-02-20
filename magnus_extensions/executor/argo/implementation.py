import logging
import shlex
from typing import Any, List, Optional, Union

from magnus import defaults, integration, utils
from magnus.executor import BaseExecutor
from magnus.graph import Graph
from magnus.nodes import BaseNode
from pydantic import BaseModel
from ruamel.yaml import YAML

logger = logging.getLogger(defaults.NAME)


class SecretEnvVar(BaseModel):
    environment_variable: str
    secret_name: str
    secret_key: str

    def dict(self, *args, **kwargs):
        return {
            "name": self.environment_variable,
            "valueFrom": {
                "secretKeyRef": {
                    "name": self.secret_name,
                    "key": self.secret_key
                }
            }
        }


class EnvVar(BaseModel):
    name: str
    value: Any

    def dict(self, *args, **kwargs):
        return_value = {
            "name": self.name
        }

        if self.value:
            return_value["value"] = self.value

        return return_value


class Resource(BaseModel):
    memory: str
    cpu: str


class Container(BaseModel):
    command: List[str]
    image: str
    limits: Resource
    requests: Resource
    imagePullPolicy: str = "IfNotPresent"
    retry: int = 1
    env: List[Union[SecretEnvVar, EnvVar]] = []

    def dict(self, *args, **kwargs) -> dict:
        return {
            "command": self.command,
            "image": self.image,
            "imagePullPolicy": self.imagePullPolicy,
            "resources": {
                "limits": self.limits.dict(),
                "requests": self.requests.dict()
            },
            "retryStrategy": {"limit": str(self.retry)},
            "env": [e.dict() for e in self.env]
        }


class TaskTemplate(BaseModel):
    name: str
    template: str
    dependencies: List[str] = []


class ContainerTemplate(BaseModel):
    name: str
    container: Container


class DagTemplate(BaseModel):
    name: str = "magnus-dag"
    tasks: List[TaskTemplate] = []

    def dict(self, *args, **kwargs):
        return {
            "name": self.name,
            "dag": {
                "tasks": [task.dict() for task in self.tasks]
            }
        }


class Spec(BaseModel):
    entrypoint: str = "magnus-dag"
    templates: Union[DagTemplate, ContainerTemplate] = []
    serviceAccountName: str = "pipeline-runner"
    arguments: List[EnvVar] = []

    def dict(self, *args, **kwargs):
        return {
            "entrypoint": self.entrypoint,
            "templates": [template.dict() for template in self.templates],
            "serviceAccountName": self.serviceAccountName,
            "arguments": {
                "parameters": [env.dict() for env in self.arguments]
            }
        }


class WorkSpec(BaseModel):
    apiVersion: str = "argoproj.io/v1alpha1"
    kind: str = "Workflow"
    metadata: dict = {"generateName": "magnus-dag-"}
    spec: Optional[Spec]


class ArgoExecutor(BaseExecutor):

    service_name = "argo"
    run_id_placeholder = "{{workflow.parameters.run_id}}"

    class Config(BaseExecutor.Config):
        docker_image: str
        output_file: str = 'pipeline.yaml'
        default_cpu_limit: str = "250m"
        default_memory_limit: str = "1G"
        default_cpu_request: str = ""
        default_memory_request: str = ""
        enable_caching: bool = False
        image_pull_policy: str = "IfNotPresent"
        secrets_from_k8s: dict = {}

    def prepare_for_graph_execution(self):
        """
        This method would be called prior to calling execute_graph.
        Perform any steps required before doing the graph execution.

        The most common implementation is to prepare a run log for the run if the run uses local interactive compute.

        But in cases of actual rendering the job specs (eg: AWS step functions, K8's) we need not do anything.
        """

        integration.validate(self, self.run_log_store)
        integration.configure_for_traversal(self, self.run_log_store)

        integration.validate(self, self.catalog_handler)
        integration.configure_for_traversal(self, self.catalog_handler)

        integration.validate(self, self.secrets_handler)
        integration.configure_for_traversal(self, self.secrets_handler)

    def prepare_for_node_execution(self):
        """
        Perform any modifications to the services prior to execution of the node.

        Args:
            node (Node): [description]
            map_variable (dict, optional): [description]. Defaults to None.
        """

        integration.validate(self, self.run_log_store)
        integration.configure_for_execution(self, self.run_log_store)

        integration.validate(self, self.catalog_handler)
        integration.configure_for_execution(self, self.catalog_handler)

        integration.validate(self, self.secrets_handler)
        integration.configure_for_execution(self, self.secrets_handler)

        self._set_up_run_log(exists_ok=True)

    def trigger_job(self, node: BaseNode, map_variable: dict = None, **kwargs):
        # TODO: This might have to be removed once core is corrected
        pass

    def execute_node(self, node: BaseNode, map_variable: dict = None, **kwargs):
        # TODO: might need implicit failure
        step_log = self.run_log_store.create_step_log(node.name, node._get_step_log_name(map_variable))

        self.add_code_identities(node=node, step_log=step_log)

        step_log.step_type = node.node_type
        step_log.status = defaults.PROCESSING
        self.run_log_store.add_step_log(step_log, self.run_id)

        super()._execute_node(node, map_variable=map_variable, **kwargs)

        # Implicit fail
        _, next_node_name = self._get_status_and_next_node_name(node, self.dag, map_variable=map_variable)
        if next_node_name:
            # Terminal nodes do not have next node name
            next_node = self.dag.get_node_by_name(next_node_name)

            if next_node.node_type == defaults.FAIL:
                self.execute_node(next_node, map_variable=map_variable)

        step_log = self.run_log_store.get_step_log(node._get_step_log_name(map_variable), self.run_id)
        if step_log.status == defaults.FAIL:
            raise Exception(f'Step {node.name} failed')

    def get_parameters(self):
        parameters = utils.get_user_set_parameters()
        if self.parameters_file:
            parameters.update(utils.load_yaml(self.parameters_file))
        return parameters

    def get_clean_name(self, node_name: str):
        return node_name.replace(" ", "-")

    def create_container_template(self, working_on: BaseNode):
        command = shlex.split(utils.get_node_execution_command(self, working_on,
                                                               over_write_run_id=self.run_id_placeholder))
        mode_config = self._resolve_node_config(working_on)
        secrets = mode_config.get("secrets_from_k8s", {})

        docker_image = mode_config['docker_image']
        cpu_limit = mode_config.get('cpu_limit', self.config.default_cpu_limit)
        memory_limit = mode_config.get('memory_limit', self.config.default_memory_limit)

        cpu_request = mode_config.get('cpu_request', self.config.default_cpu_request) or cpu_limit
        memory_request = mode_config.get('memory_request', self.config.default_memory_request) or memory_limit

        request = Resource(memory=memory_request, cpu=cpu_request)
        limits = Resource(memory=memory_limit, cpu=cpu_limit)

        image_pull_policy = mode_config.get("image_pull_policy", self.config.image_pull_policy)

        container = Container(command=command, image=docker_image, limits=limits, requests=request,
                              imagePullPolicy=image_pull_policy, retry=working_on._get_max_attempts())
        for secret_env, k8_secret in secrets.items():
            try:
                secret_name, key = k8_secret.split(':')
            except Exception as _e:
                msg = (
                    "K8's secret should be of format EnvVar=SecretName:Key"
                )
                raise Exception(msg) from _e
            secret = SecretEnvVar(environment_variable=secret_env, secret_name=secret_name, secret_key=key)
            container.env.append(secret)

        if working_on.name == self.dag.start_at:
            for key, _ in self.get_parameters().items():
                # Get the value from work flow parameters for dynamic behavior
                env_var = EnvVar(name=defaults.PARAMETER_PREFIX + key, value="{{workflow.parameters." + key + "}}")
                container.env.append(env_var)

        container_template = ContainerTemplate(name=self.get_clean_name(working_on.name), container=container)

        return container_template

    def get_templates(self, dag):
        current_node = dag.start_at
        previous_node = None

        container_templates = {}
        task_templates = {}
        while True:
            working_on = dag.get_node_by_name(current_node)
            if working_on.is_composite:
                raise NotImplementedError('Composite nodes are not yet implemented')

            if previous_node == current_node:
                raise Exception('Potentially running in a infinite loop')

            container_template = self.create_container_template(working_on=working_on)

            if current_node not in container_templates:
                container_templates[current_node] = container_template

            clean_name = self.get_clean_name(working_on.name)
            task_template = TaskTemplate(name=clean_name, template=clean_name)
            if previous_node:
                task_template.dependencies.append(self.get_clean_name(previous_node))

            task_templates[current_node] = task_template

            previous_node = current_node

            if working_on.node_type in ['success', 'fail']:
                break

            current_node = working_on._get_next_node()

        return [template for k, template in container_templates.items()], \
            [template for k, template in task_templates.items()]

    def execute_graph(self, dag: Graph, map_variable: dict = None, **kwargs):
        workspec = WorkSpec()
        specification = Spec()
        workspec.spec = specification

        for key, value in self.get_parameters().items():
            # Get the value from work flow parameters for dynamic behavior
            env_var = EnvVar(name=key, value=value)
            specification.arguments.append(env_var)

        run_id_var = EnvVar(name="run_id", value="{{workflow.uid}}")
        specification.arguments.append(run_id_var)

        container_templates, task_templates = self.get_templates(dag=dag)
        specification.templates.extend(container_templates)
        dag_template = DagTemplate(tasks=task_templates)
        specification.templates.extend([dag_template])
        yaml = YAML()
        with open(self.config.output_file, 'w') as f:
            yaml.dump(workspec.dict(), f)

    def send_return_code(self, stage='traversal'):
        """
        Convenience function used by pipeline to send return code to the caller of the cli

        Raises:
            Exception: If the pipeline execution failed
        """
        if stage != 'traversal':  # traversal does no actual execution, so return code is pointless
            run_id = self.run_id

            run_log = self.run_log_store.get_run_log_by_id(run_id=run_id, full=False)
            if run_log.status == defaults.FAIL:
                raise Exception('Pipeline execution failed')
