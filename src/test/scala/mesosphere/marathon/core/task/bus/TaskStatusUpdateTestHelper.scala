package mesosphere.marathon.core.task.bus

import mesosphere.marathon.core.instance.update._
import mesosphere.marathon.core.instance.{ Instance, InstanceStatus, TestInstanceBuilder }
import mesosphere.marathon.core.pod.MesosContainer
import mesosphere.marathon.core.task.{ MarathonTaskStatus, Task }
import mesosphere.marathon.state.{ PathId, Timestamp }
import org.apache.mesos.Protos.TaskStatus.Reason
import org.apache.mesos.Protos.{ TaskState, TaskStatus }
import org.joda.time.DateTime
import org.slf4j.LoggerFactory

class TaskStatusUpdateTestHelper(val operation: InstanceUpdateOperation, val effect: InstanceUpdateEffect) {
  def simpleName = operation match {
    case InstanceUpdateOperation.MesosUpdate(_, marathonTaskStatus, mesosStatus, _) =>
      mesosStatus.getState.toString
    case _ => operation.getClass.getSimpleName
  }
  def status = operation match {
    case InstanceUpdateOperation.MesosUpdate(_, marathonTaskStatus, mesosStatus, _) => mesosStatus
    case _ => throw new scala.RuntimeException("the wrapped stateOp os no MesosUpdate!")
  }
  def reason: String = if (status.hasReason) status.getReason.toString else "no reason"
  def wrapped: InstanceChange = effect match {
    case InstanceUpdateEffect.Update(instance, old, events) => InstanceUpdated(instance, old.map(_.state), events)
    case InstanceUpdateEffect.Expunge(instance, events) => InstanceDeleted(instance, None, events)
    case _ => throw new scala.RuntimeException(s"The wrapped effect does not result in an update or expunge: $effect")
  }
  private[this] def instanceFromOperation: Instance = operation match {
    case launch: InstanceUpdateOperation.LaunchEphemeral => launch.instance
    case update: InstanceUpdateOperation.MesosUpdate => update.instance
    case _ => throw new RuntimeException(s"Unable to fetch instance from ${operation.getClass.getSimpleName}")
  }
  def updatedInstance: Instance = effect match {
    case InstanceUpdateEffect.Update(instance, old, events) => instance
    case InstanceUpdateEffect.Expunge(instance, events) => instance
    case _ => instanceFromOperation
  }

}

object TaskStatusUpdateTestHelper {
  val log = LoggerFactory.getLogger(getClass)
  def apply(operation: InstanceUpdateOperation, effect: InstanceUpdateEffect): TaskStatusUpdateTestHelper =
    new TaskStatusUpdateTestHelper(operation, effect)

  lazy val defaultInstance = TestInstanceBuilder.newBuilder(PathId("/app")).addTaskStaged().getInstance()
  lazy val defaultTimestamp = Timestamp.apply(new DateTime(2015, 2, 3, 12, 30, 0, 0))

  def taskLaunchFor(instance: Instance) = {
    val operation = InstanceUpdateOperation.LaunchEphemeral(instance)
    val effect = InstanceUpdateEffect.Update(operation.instance, oldState = None, events = Nil)
    TaskStatusUpdateTestHelper(operation, effect)
  }

  def taskUpdateFor(instance: Instance, taskStatus: InstanceStatus, mesosStatus: TaskStatus, timestamp: Timestamp = defaultTimestamp) = {
    val operation = InstanceUpdateOperation.MesosUpdate(instance, taskStatus, mesosStatus, timestamp)
    val effect = operation.instance.update(operation)
    TaskStatusUpdateTestHelper(operation, effect)
  }

  def taskExpungeFor(instance: Instance, taskStatus: InstanceStatus, mesosStatus: TaskStatus, timestamp: Timestamp = defaultTimestamp) = {
    val operation = InstanceUpdateOperation.MesosUpdate(instance, taskStatus, mesosStatus, timestamp)
    val effect = operation.instance.update(operation)
    if (!effect.isInstanceOf[InstanceUpdateEffect.Expunge]) {
      throw new RuntimeException(s"Applying a MesosUpdate with status $taskStatus did not result in an Expunge effect but in a $effect")
    }
    TaskStatusUpdateTestHelper(operation, effect)
  }

  def running(instance: Instance = defaultInstance, container: Option[MesosContainer] = None) = {
    val taskId = Task.Id.forInstanceId(instance.instanceId, container)
    val status = MesosTaskStatusTestHelper.running(taskId)
    taskUpdateFor(instance, InstanceStatus.Running, status)
  }

  def runningHealthy(instance: Instance = defaultInstance, container: Option[MesosContainer] = None) = {
    val taskId = Task.Id.forInstanceId(instance.instanceId, container)
    val status = MesosTaskStatusTestHelper.runningHealthy(taskId)
    taskUpdateFor(instance, InstanceStatus.Running, status)
  }

  def runningUnhealthy(instance: Instance = defaultInstance, container: Option[MesosContainer] = None) = {
    val taskId = Task.Id.forInstanceId(instance.instanceId, container)
    val status = MesosTaskStatusTestHelper.runningUnhealthy(taskId)
    taskUpdateFor(instance, InstanceStatus.Running, status)
  }

  def staging(instance: Instance = defaultInstance) = {
    val taskId = Task.Id.forInstanceId(instance.instanceId, None)
    val status = MesosTaskStatusTestHelper.staging(taskId)
    taskUpdateFor(instance, InstanceStatus.Staging, status)
  }

  def finished(instance: Instance = defaultInstance) = {
    val taskId = Task.Id.forInstanceId(instance.instanceId, None)
    val status = MesosTaskStatusTestHelper.finished(taskId)
    taskExpungeFor(instance, InstanceStatus.Finished, status)
  }

  def lost(reason: Reason, instance: Instance = defaultInstance, maybeMessage: Option[String] = None) = {
    val taskId = Task.Id.forInstanceId(instance.instanceId, None)
    val mesosStatus = MesosTaskStatusTestHelper.mesosStatus(state = TaskState.TASK_LOST,
      maybeReason = Some(reason), maybeMessage = maybeMessage)
    val marathonTaskStatus = MarathonTaskStatus(mesosStatus)

    marathonTaskStatus match {
      case _: InstanceStatus.Terminal =>
        taskExpungeFor(instance, marathonTaskStatus, mesosStatus)

      case _ =>
        taskUpdateFor(instance, marathonTaskStatus, mesosStatus)
    }
  }

  def unreachable(instance: Instance = defaultInstance) = {
    val mesosStatus = MesosTaskStatusTestHelper.unreachable(Task.Id.forInstanceId(instance.instanceId, None))
    val marathonTaskStatus = MarathonTaskStatus(mesosStatus)

    marathonTaskStatus match {
      case _: InstanceStatus.Terminal =>
        taskExpungeFor(instance, marathonTaskStatus, mesosStatus)

      case _ =>
        taskUpdateFor(instance, marathonTaskStatus, mesosStatus)
    }
  }

  def killed(instance: Instance = defaultInstance) = {
    // TODO(PODS): the method signature should allow passing a taskId
    val taskId = instance.tasks.head.taskId
    val status = MesosTaskStatusTestHelper.killed(taskId)
    taskExpungeFor(instance, InstanceStatus.Killed, status)
  }

  def killing(instance: Instance = defaultInstance) = {
    val status = MesosTaskStatusTestHelper.killing(Task.Id.forInstanceId(instance.instanceId, None))
    taskUpdateFor(instance, InstanceStatus.Killing, status)
  }

  def error(instance: Instance = defaultInstance) = {
    val status = MesosTaskStatusTestHelper.error(Task.Id.forInstanceId(instance.instanceId, None))
    taskExpungeFor(instance, InstanceStatus.Error, status)
  }

  //TODO(kjeschkies): I don't think these updates should be expunge updates. What's the distinction?
  def failed(instance: Instance = defaultInstance) = {
    val status = MesosTaskStatusTestHelper.failed(Task.Id.forInstanceId(instance.instanceId, None))
    taskExpungeFor(instance, InstanceStatus.Failed, status)
  }

  def gone(instance: Instance = defaultInstance) = {
    val status = MesosTaskStatusTestHelper.gone(Task.Id.forInstanceId(instance.instanceId, None))
    taskExpungeFor(instance, InstanceStatus.Gone, status)
  }

  def dropped(instance: Instance = defaultInstance) = {
    val status = MesosTaskStatusTestHelper.dropped(Task.Id.forInstanceId(instance.instanceId, None))
    taskExpungeFor(instance, InstanceStatus.Dropped, status)
  }

  def unknown(instance: Instance = defaultInstance) = {
    val status = MesosTaskStatusTestHelper.unknown(Task.Id.forInstanceId(instance.instanceId, None))
    taskExpungeFor(instance, InstanceStatus.Unknown, status)
  }
}
