package mesosphere.marathon.api

import mesosphere.marathon._
import mesosphere.marathon.core.group.GroupManager
import mesosphere.marathon.core.instance.{ Instance, TestInstanceBuilder }
import mesosphere.marathon.core.instance.update.{ InstanceUpdateEffect, InstanceUpdateOperation }
import mesosphere.marathon.core.task.tracker.{ InstanceTracker, TaskStateOpProcessor }
import mesosphere.marathon.state.{ AppDefinition, Group, PathId, Timestamp }
import mesosphere.marathon.test.{ MarathonSpec, Mockito }
import mesosphere.marathon.upgrade.DeploymentPlan
import org.mockito.ArgumentCaptor
import org.mockito.Mockito._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{ BeforeAndAfterAll, GivenWhenThen, Matchers }

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._

class TaskKillerTest extends MarathonSpec
    with Matchers
    with BeforeAndAfterAll
    with GivenWhenThen
    with MockitoSugar
    with Mockito
    with ScalaFutures {

  val auth: TestAuthFixture = new TestAuthFixture
  implicit val identity = auth.identity

  //regression for #3251
  test("No tasks to kill should return with an empty array") {
    val f = new Fixture
    val appId = PathId("invalid")
    when(f.tracker.specInstances(appId)).thenReturn(Future.successful(Iterable.empty))
    when(f.groupManager.runSpec(appId)).thenReturn(Future.successful(Some(AppDefinition(appId))))

    val result = f.taskKiller.kill(appId, (tasks) => Set.empty[Instance]).futureValue
    result.isEmpty shouldEqual true
  }

  test("AppNotFound") {
    val f = new Fixture
    val appId = PathId("invalid")
    when(f.tracker.specInstances(appId)).thenReturn(Future.successful(Iterable.empty))
    when(f.groupManager.runSpec(appId)).thenReturn(Future.successful(None))

    val result = f.taskKiller.kill(appId, (tasks) => Set.empty[Instance])
    result.failed.futureValue shouldEqual UnknownAppException(appId)
  }

  test("AppNotFound with scaling") {
    val f = new Fixture
    val appId = PathId("invalid")
    when(f.tracker.hasSpecInstancesSync(appId)).thenReturn(false)

    val result = f.taskKiller.killAndScale(appId, (tasks) => Set.empty[Instance], force = true)
    result.failed.futureValue shouldEqual UnknownAppException(appId)
  }

  test("KillRequested with scaling") {
    val f = new Fixture
    val appId = PathId(List("app"))
    val instance1 = TestInstanceBuilder.newBuilder(appId).addTaskRunning().getInstance()
    val instance2 = TestInstanceBuilder.newBuilder(appId).addTaskRunning().getInstance()
    val tasksToKill: Iterable[Instance] = Set(instance1, instance2)

    when(f.tracker.hasSpecInstancesSync(appId)).thenReturn(true)
    when(f.groupManager.group(appId.parent)).thenReturn(Future.successful(Some(Group.emptyWithId(appId.parent))))

    val groupUpdateCaptor = ArgumentCaptor.forClass(classOf[(Group) => Group])
    val forceCaptor = ArgumentCaptor.forClass(classOf[Boolean])
    val toKillCaptor = ArgumentCaptor.forClass(classOf[Map[PathId, Iterable[Instance]]])
    val expectedDeploymentPlan = DeploymentPlan.empty
    when(f.groupManager.update(
      any[PathId],
      groupUpdateCaptor.capture(),
      any[Timestamp],
      forceCaptor.capture(),
      toKillCaptor.capture())
    ).thenReturn(Future.successful(expectedDeploymentPlan))

    val result = f.taskKiller.killAndScale(appId, (tasks) => tasksToKill, force = true)
    result.futureValue shouldEqual expectedDeploymentPlan
    forceCaptor.getValue shouldEqual true
    toKillCaptor.getValue shouldEqual Map(appId -> tasksToKill)
  }

  test("KillRequested without scaling") {
    val f = new Fixture
    val appId = PathId(List("my", "app"))
    val instance = TestInstanceBuilder.newBuilder(appId).addTaskRunning().getInstance()
    val tasksToKill: Iterable[Instance] = Set(instance)
    when(f.groupManager.runSpec(appId)).thenReturn(Future.successful(Some(AppDefinition(appId))))
    when(f.tracker.specInstances(appId)).thenReturn(Future.successful(tasksToKill))
    when(f.service.killTasks(appId, tasksToKill)).thenReturn(Future.successful(MarathonSchedulerActor.TasksKilled(appId, tasksToKill.map(_.instanceId))))

    val result = f.taskKiller.kill(appId, { tasks =>
      tasks should equal(tasksToKill)
      tasksToKill
    })

    result.futureValue shouldEqual tasksToKill
    verify(f.service, times(1)).killTasks(appId, tasksToKill)
  }

  test("Fail when one task kill fails") {
    Given("An app with several tasks")
    val f = new Fixture
    val appId = PathId(List("my", "app"))
    val tasksToKill: Iterable[Instance] = Set(
      TestInstanceBuilder.newBuilder(appId).addTaskRunning().getInstance(),
      TestInstanceBuilder.newBuilder(appId).addTaskRunning().getInstance()
    )
    when(f.groupManager.runSpec(appId)).thenReturn(Future.successful(Some(AppDefinition(appId))))
    when(f.tracker.specInstances(appId)).thenReturn(Future.successful(tasksToKill))
    when(f.service.killTasks(appId, tasksToKill)).thenReturn(Future.failed(AppLockedException()))

    When("TaskKiller kills all tasks")
    val result = f.taskKiller.kill(appId, { tasks =>
      tasks should equal(tasksToKill)
      tasksToKill
    })

    Then("The kill should fail.")
    result.failed.futureValue shouldEqual AppLockedException()
    verify(f.service, times(1)).killTasks(appId, tasksToKill)
  }

  test("Kill and scale w/o force should fail if there is a deployment") {
    val f = new Fixture
    val appId = PathId(List("my", "app"))
    val instance1 = TestInstanceBuilder.newBuilder(appId).addTaskRunning().getInstance()
    val instance2 = TestInstanceBuilder.newBuilder(appId).addTaskRunning().getInstance()
    val tasksToKill = Set(instance1, instance2)

    when(f.tracker.hasSpecInstancesSync(appId)).thenReturn(true)
    when(f.groupManager.group(appId.parent)).thenReturn(Future.successful(Some(Group.emptyWithId(appId.parent))))
    val groupUpdateCaptor = ArgumentCaptor.forClass(classOf[(Group) => Group])
    val forceCaptor = ArgumentCaptor.forClass(classOf[Boolean])
    when(f.groupManager.update(
      any[PathId],
      groupUpdateCaptor.capture(),
      any[Timestamp],
      forceCaptor.capture(),
      any[Map[PathId, Iterable[Instance]]]
    )).thenReturn(Future.failed(AppLockedException()))

    val result = f.taskKiller.killAndScale(appId, (tasks) => tasksToKill, force = false)
    forceCaptor.getValue shouldEqual false
    result.failed.futureValue shouldEqual AppLockedException()
  }

  test("kill with wipe will kill running and expunge all") {
    val f = new Fixture
    val appId = PathId(List("my", "app"))
    val runningInstance: Instance = TestInstanceBuilder.newBuilder(appId).addTaskRunning().getInstance()
    val reservedInstance: Instance = TestInstanceBuilder.newBuilder(appId).addTaskReserved().getInstance()
    val instancesToKill: Iterable[Instance] = Set(runningInstance, reservedInstance)
    val launchedInstances = Set(runningInstance)
    val expungeRunning = InstanceUpdateOperation.ForceExpunge(runningInstance.instanceId)
    val expungeReserved = InstanceUpdateOperation.ForceExpunge(reservedInstance.instanceId)

    when(f.groupManager.runSpec(appId)).thenReturn(Future.successful(Some(AppDefinition(appId))))
    when(f.tracker.specInstances(appId)).thenReturn(Future.successful(instancesToKill))
    when(f.stateOpProcessor.process(expungeRunning)).thenReturn(Future.successful(InstanceUpdateEffect.Expunge(runningInstance, events = Nil)))
    when(f.stateOpProcessor.process(expungeReserved)).thenReturn(Future.successful(InstanceUpdateEffect.Expunge(reservedInstance, events = Nil)))
    when(f.service.killTasks(appId, launchedInstances))
      .thenReturn(Future.successful(MarathonSchedulerActor.TasksKilled(appId, launchedInstances.map(_.instanceId))))

    val result = f.taskKiller.kill(appId, { tasks =>
      tasks should equal(instancesToKill)
      instancesToKill
    }, wipe = true)
    result.futureValue shouldEqual instancesToKill
    // only task1 is killed
    verify(f.service, times(1)).killTasks(appId, launchedInstances)
    // all found instances are expunged and the launched instance is eventually expunged again
    verify(f.stateOpProcessor, atLeastOnce).process(expungeRunning)
    verify(f.stateOpProcessor).process(expungeReserved)
  }

  class Fixture {
    val tracker: InstanceTracker = mock[InstanceTracker]
    val stateOpProcessor: TaskStateOpProcessor = mock[TaskStateOpProcessor]
    val service: MarathonSchedulerService = mock[MarathonSchedulerService]
    val groupManager: GroupManager = mock[GroupManager]

    val config: MarathonConf = mock[MarathonConf]
    when(config.zkTimeoutDuration).thenReturn(1.second)

    val taskKiller: TaskKiller = new TaskKiller(tracker, stateOpProcessor, groupManager, service, config, auth.auth, auth.auth)
  }

}
