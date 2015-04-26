package org.vr

import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.ActorContext

/**
 * @author hgadre
 */
trait VrMessage
trait RequestProcessingMessage extends VrMessage
case class ClientRequest(clientId: String, requestId: Integer) extends RequestProcessingMessage
case class ClientReply(requestId: Integer, success: Boolean) extends RequestProcessingMessage
case class Prepare(viewId: Integer, operation: ClientRequest, opId: Integer, commitId: Integer) extends RequestProcessingMessage
case class PrepareOK(viewId: Int, opId: Int, replicaId: Int) extends RequestProcessingMessage
case class Commit(viewId: Integer, commitId: Integer) extends RequestProcessingMessage
//Used specifically for testing and debugging.
case object QueryReplicaState extends VrMessage

case class QuorumCheck(clientId: String, requestId: Int)
object SendCommit

sealed trait RequestStatus
case object Initialized extends RequestStatus
case object PrepareSent extends RequestStatus
case object PrepareTimeout extends RequestStatus
case object QuorumReplied extends RequestStatus
case object Committed extends RequestStatus
case class RequestState(id: Integer, opId: Int, status: RequestStatus,
    clientRef: Option[ActorRef], req: ClientRequest,
    reply: ClientReply = null, acks: Set[Int] = Set()) {
  def isCommitInProgress: Boolean = (status == PrepareSent)
  def isCommitNotInformed: Boolean = (status == QuorumReplied)
}

case class CommitLog(opId: Int, req: ClientRequest)

case class ReplicaState(replicaCount: Int, replicaNum: Int, viewId: Int, opId: Int, commitId: Int, log: List[CommitLog], reqTracker: Map[String, RequestState]) {
  def isCommitInProgress: Boolean = reqTracker.exists({ case (_, y: RequestState) => y.isCommitInProgress });

  def isCommitNotInfomred: Boolean = reqTracker.exists({ case (_, y: RequestState) => y.isCommitNotInformed })

  def findRequestForOpId(opId: Int): Option[RequestState] = reqTracker.values.find { x => x.opId == opId }

  def amILeader: Boolean = (replicaNum == 1)

  def findRequestForCommitId(commitId: Int): Option[RequestState] = findRequestForOpId(commitId) flatMap { state =>
    state.status match {
      case Initialized => Some(state)
      //This can't happen. The only valid state is Initialized
      case _ => None
    }
  }

  def handleClientRequest(clientRef: ActorRef, req: ClientRequest): (Option[VrMessage], ReplicaState) = {
    reqTracker.get(req.clientId) match {
      case Some(RequestState(reqId, _, _, _, _, reply, _)) =>
        if (req.requestId < reqId) (None, this)
        else if (req.requestId == reqId) (Some(reply), this)
        else createNewRequest(clientRef, req)
      case None =>
        createNewRequest(clientRef, req)
    }
  }

  def createNewRequest(clientRef: ActorRef, req: ClientRequest): (Option[VrMessage], ReplicaState) = {
    val newOpId = this.opId + 1;
    val newState = org.vr.RequestState(req.requestId, newOpId, PrepareSent, Some(clientRef), req)
    val newTracker = reqTracker.map({
      case (cId, state) =>
        //Mark all previous successful responses as committed.
        if (state.status == QuorumReplied) (cId, state.copy(status = Committed))
        else (cId, state)
    }) + (req.clientId -> newState)

    val prepMsg = Prepare(viewId, req, newOpId, commitId)
    (Some(prepMsg), this.copy(opId = newOpId, reqTracker = newTracker))
  }

  def handlePrepare(req: Prepare): (Option[VrMessage], ReplicaState) = {
    if (amILeader || (opId + 1) == req.opId) {
      //Record the client request in the transaction logs
      val newLog = log ++ List(CommitLog(req.opId, req.operation))
      val replyMsg = PrepareOK(req.viewId, req.opId, replicaNum)
      val reqState = RequestState(req.operation.requestId, req.opId, Initialized, None, req.operation)
      //TODO handle commitId.
      (Some(replyMsg), this.copy(opId = opId + 1, log = newLog, reqTracker = reqTracker + (req.operation.clientId -> reqState)))
    } else {
      //TODO - Handle the case of missing operations (i.e. node recovery).
      (None, this)
    }
  }

  def handleLocalPrepare(clientId: String, req: Prepare): ReplicaState = {
    //Record the client request in the transaction logs
    val newLog = log ++ List(CommitLog(req.opId, req.operation))
    val newTracker = this.reqTracker.map({
      case (cId, rstate) =>
        if (cId == clientId) (cId, rstate.copy(acks = rstate.acks ++ Set(replicaNum)))
        else (cId, rstate)
    })
    this.copy(log = newLog, reqTracker = newTracker)
  }

  def handlePrepareOK(rsp: PrepareOK): (Option[RequestState], ReplicaState) = {
    findRequestForOpId(rsp.opId) flatMap { state =>
      state.status match {
        case PrepareSent =>
          val newAcks = state.acks ++ Set(rsp.replicaId)
          val newStatus = if (newAcks.size > (replicaCount / 2)) QuorumReplied else PrepareSent
          val newState = state.copy(status = newStatus, acks = newAcks)
          val newCommitId = if (newStatus == QuorumReplied) state.opId else this.commitId
          Some(Some(newState), this.copy(commitId = newCommitId, reqTracker = reqTracker + (state.req.clientId -> newState)))
        case _ =>
          Some(None, this)
      }
    } getOrElse ((None, this))
  }

  def handlePrepareTimeout(clientId: String, requestId: Int): (Option[ActorRef], Option[VrMessage], ReplicaState) = reqTracker.get(clientId) match {
    case Some(RequestState(id, _, PrepareSent, Some(clientRef), _, _, _)) => {
      if (id == requestId) {
        val newstate = reqTracker.get(clientId).get.copy(status = PrepareTimeout)
        (Some(clientRef), Some(ClientReply(id, false)), this.copy(reqTracker = reqTracker + (clientId -> newstate)))
      } else {
        (None, None, this)
      }
    }
    case _ => (None, None, this)
  }

  def flushCommits(commitId: Int): ReplicaState = this.copy(reqTracker = reqTracker.map({
    case (cId, rstate) =>
      if (rstate.opId <= commitId && rstate.status == QuorumReplied) (cId, rstate.copy(status = Committed))
      else (cId, rstate)
  }))

  def handleCommit(commitId: Int, requestId: Int, result: ClientReply): ReplicaState = {
    this.copy(commitId = commitId, reqTracker = reqTracker.map({
      case (cId, rstate) =>
        if (rstate.id == requestId) (cId, rstate.copy(status = Committed, reply = result)) else (cId, rstate)
    }))
  }

  def updateResult(state: RequestState, result: ClientReply): ReplicaState = {
    val newState = state.copy(reply = result)
    this.copy(reqTracker = reqTracker + (state.req.clientId -> newState))
  }
}