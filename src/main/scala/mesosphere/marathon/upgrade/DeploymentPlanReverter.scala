package mesosphere.marathon.upgrade

import mesosphere.marathon.core.pod.PodDefinition
import mesosphere.marathon.state._
import org.slf4j.LoggerFactory

import scala.collection.immutable.Seq

/**
  * Contains the logic to revert deployments by calculating the changes
  * of the deployment and applying the reverse on the given group.
  */
private[upgrade] object DeploymentPlanReverter {
  private[this] val log = LoggerFactory.getLogger(getClass)

  /**
    * Reverts this plan by applying the reverse changes to the given Group.
    *
    * If they were no concurrent deployments and the passed group is equal to
    * the target group, this method should result in the original group
    * except for version/timestamp changes.
    *
    * If there were concurrent changes, this method tries to revert the changes
    * of this deployment only without affecting the other deployments.
    */
  def revert(original: Group, target: Group, newVersion: Timestamp = Timestamp.now()): Group => Group = {

    def changesOnIds[T](originalSet: Set[T], targetSet: Set[T])(id: T => PathId): Seq[(Option[T], Option[T])] = {
      def mapById(entities: Set[T]): Map[PathId, T] =
        entities.map { entity => id(entity) -> entity }.toMap

      val originalById = mapById(originalSet)
      val targetById = mapById(targetSet)

      val ids = originalById.keys ++ targetById.keys

      ids
        .iterator
        .map { id => originalById.get(id) -> targetById.get(id) }
        .to[Seq]
    }

    /* a sequence of tuples with the old and the new group definition (also for unchanged groups) */
    val groupChanges: Seq[(Option[Group], Option[Group])] =
      changesOnIds(original.transitiveGroups, target.transitiveGroups)(_.id)

    /* a sequence of tuples with the old and the new run definition */
    val runSpecChanges: Seq[(Option[RunSpec], Option[RunSpec])] = {
      changesOnIds(original.transitiveRunSpecs, target.transitiveRunSpecs)(_.id)
        .filter { case (oldOpt, newOpt) => oldOpt != newOpt }
    }

    // We need to revert run changes first so that runs have already been deleted when we check
    // a group is empty and can be removed.
    (revertRunSpecChanges(newVersion, runSpecChanges) _).andThen(revertGroupChanges(newVersion, groupChanges))
  }

  /**
    * Reverts any group additions, changes, removals.
    *
    * It is more difficult than reverting any app definition changes
    * because groups are not locked by deployments and concurrent changes are allowed.
    */
  private[this] def revertGroupChanges(
    version: Timestamp, groupChanges: Seq[(Option[Group], Option[Group])])(
    group: Group): Group = {

    def revertGroupRemoval(oldGroup: Group)(existingGroup: Group): Group = {
      log.debug("re-adding group {} with dependencies {}", Seq(oldGroup.id, oldGroup.dependencies): _*)
      if ((oldGroup.dependencies -- existingGroup.dependencies).nonEmpty) {
        existingGroup.copy(dependencies = existingGroup.dependencies ++ oldGroup.dependencies)
      } else {
        existingGroup
      }
    }

    def revertDependencyChanges(oldGroup: Group, newGroup: Group)(group: Group): Group = {
      val removedDependencies = oldGroup.dependencies -- newGroup.dependencies
      val addedDependencies = newGroup.dependencies -- oldGroup.dependencies

      if (removedDependencies.nonEmpty || addedDependencies.nonEmpty) {
        if (log.isDebugEnabled)
          log.debug(
            s"revert dependency changes in group ${oldGroup.id}, " +
              s"readding removed {${removedDependencies.mkString(", ")}}, " +
              s"removing added {${addedDependencies.mkString(", ")}}")

        group.copy(dependencies = group.dependencies ++ removedDependencies -- addedDependencies)
      } else {
        // common case, unchanged
        group
      }
    }

    def revertGroupAddition(result: Group, newGroup: Group): Group = {
      // We know that all group removals for groups inside of this group have been
      // performed before. If there are no conflicts with other deployments, we could
      // just remove the group. Unfortunately, another deployment could have
      // added a new sub group or sub app definition. In this case, we should not remove
      // this group.

      def normalized(group: Group): Group = group.withNormalizedVersion.withoutChildren
      def isGroupUnchanged(group: Group): Boolean =
        !group.containsAppsOrPodsOrGroups && normalized(group) == normalized(newGroup)

      result.group(newGroup.id) match {
        case Some(unchanged) if isGroupUnchanged(unchanged) =>

          log.debug("remove unchanged group {}", unchanged.id)
          result.remove(unchanged.id, version)
        case _ if newGroup.dependencies.nonEmpty =>
          // group dependencies have changed
          if (log.isDebugEnabled)
            log.debug(s"group ${newGroup.id} has changed. " +
              s"Removed added dependencies ${newGroup.dependencies.mkString(", ")}")
          result.update(
            newGroup.id,
            group => group.copy(dependencies = group.dependencies -- newGroup.dependencies),
            version)
        case _ =>
          // still contains apps/groups, so we keep it
          result
      }
    }

    // sort groups so that inner groups are processed first
    val sortedGroupChanges = groupChanges.sortWith {
      case (change1, change2) =>
        // both groups are supposed to have the same path id (if there are any)
        def pathId(change: (Option[Group], Option[Group])): PathId = {
          Seq(change._1, change._2).flatten.headOption.fold(PathId.empty)(_.id)
        }

        pathId(change1) > pathId(change2)
    }

    sortedGroupChanges.foldLeft(group) {
      case (result, groupUpdate) =>
        groupUpdate match {
          case (Some(oldGroup), None) =>
            result.update(oldGroup.id, revertGroupRemoval(oldGroup), version)

          case (Some(oldGroup), Some(newGroup)) =>
            result.update(oldGroup.id, revertDependencyChanges(oldGroup, newGroup), version)

          case (None, Some(newGroup)) =>
            revertGroupAddition(result, newGroup)

          case (None, None) =>
            log.warn("processing unexpected NOOP in group changes")
            result
        }
    }
  }

  /**
    * Reverts app and pod additions, changes and removals.
    *
    * The logic is quite simple because during a deployment apps are locked which
    * prevents any concurrent changes.
    */
  private[this] def revertRunSpecChanges(
    version: Timestamp, changes: Seq[(Option[RunSpec], Option[RunSpec])])(
    g: Group): Group = {

    def appOrPodChange(
      runnableSpec: RunSpec,
      appChange: AppDefinition => Group,
      podChange: PodDefinition => Group): Group = runnableSpec match {
      case app: AppDefinition => appChange(app)
      case pod: PodDefinition => podChange(pod)
    }

    changes.foldLeft(g) {
      case (result, runUpdate) =>
        runUpdate match {
          case (Some(oldRun), _) => //removal or change
            log.debug("revert to old app definition {}", oldRun.id)
            appOrPodChange(
              oldRun,
              app => result.updateApp(app.id, _ => app, version),
              pod => result.updatePod(pod.id, _ => pod, version))
          case (None, Some(newRun)) =>
            log.debug("remove app definition {}", newRun.id)
            appOrPodChange(
              newRun,
              app => result.update(app.id.parent, _.removeApplication(app.id), version),
              pod => result.update(pod.id.parent, _.removePod(pod.id), version))
          case (None, None) =>
            log.warn("processing unexpected NOOP in app changes")
            result
        }
    }
  }
}
