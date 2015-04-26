package vr

import akka.actor.LoggingFSM
import akka.actor.Props
import akka.actor.ActorSystem
import akka.actor.FSM._
import akka.pattern._
import scala.concurrent.duration._
import scala.concurrent.Await
import akka.actor.ActorSelection.toScala
import akka.actor.actorRef2Scala
import akka.util.Timeout.durationToTimeout
import org.vr.ClientReply
import org.vr.ClientRequest
import org.vr.Commit
import org.vr.Prepare
import org.vr.PrepareOK
import org.vr.PrepareSent
import org.vr.QueryReplicaState
import org.vr.QuorumCheck
import org.vr.QuorumReplied
import org.vr.ReplicaState
import org.vr.RequestState
import org.vr.SendCommit
import org.vr.VrMessage

/**
 * @author hgadre
 */
sealed trait ReplicaStatus
case object Normal extends ReplicaStatus
case object Recovering extends ReplicaStatus
case object ViewChange extends ReplicaStatus

object ReplicaFSM {
  def props(replicaCount: Int, replicaNum: Int): Props = Props(new ReplicaFSM(replicaCount, replicaNum))
}

class ReplicaFSM(replicaCount: Int, replicaNum: Int) extends LoggingFSM[ReplicaStatus, ReplicaState] {
  import context.dispatcher
  
  startWith(Normal, ReplicaState(replicaCount, replicaNum, 0, 0, 0, List(), Map()))

  when(Normal) {
    case Event(req: ClientRequest, state: ReplicaState) => {
      state.handleClientRequest(sender, req) match {
        case (Some(reply: ClientReply), newstate) =>
          sender ! reply
          stay using newstate
        case (Some(msg: Prepare), newstate) =>
          //Add a commit log (i.e. short-circuit the PREPARE request for self).
          val latest = newstate.handleLocalPrepare(req.clientId, msg)
          log.info("Sending PREPARE message for request {}", req)
          broadcastMsg(msg)
          //Keep track of timeouts to for quorum to succeed.
          context.system.scheduler.scheduleOnce(5 seconds, self, QuorumCheck(req.clientId, req.requestId))
          log.info("State after client request {}", latest)
          stay using latest
        case _ =>
          stay using state
      }
    }
    
    case Event(SendCommit, state: ReplicaState) => {
      if(state.amILeader && state.isCommitNotInfomred) {
        log.info("Flusing the commits upto commit_id {}", state.commitId)
        broadcastMsg(Commit(state.viewId, state.commitId))
        stay using state.flushCommits(state.commitId)
      } else {
        stay using state
      }
    }

    case Event(msg: Prepare, state: ReplicaState) => {
      state.handlePrepare(msg) match {
        case ((Some(reply: VrMessage), newstate)) =>
          log.info("Replying PREPARE message with {}", reply)
          sender ! reply
          stay using newstate
        case _ =>
          stay using state
      }
    }

    case Event(msg: PrepareOK, state: ReplicaState) => state.handlePrepareOK(msg) match {
      case ((Some(RequestState(id, opId, QuorumReplied, Some(clientRef), req, reply, ack)), newstate)) =>
        log.info("Quorum achieved for request {} with opId {}", req, msg.opId)
        //TODO - Invoke Application logic        
        //Send reply to the client.
        clientRef ! ClientReply(id, true)        
        stay using newstate.updateResult(RequestState(id, opId, QuorumReplied, Some(clientRef), req, reply, ack), ClientReply(id, true))
      case ((Some(RequestState(_, _, PrepareSent, _, _, _, _)), newstate)) =>
        stay using newstate
      case x =>
        stay using state
    }
    
    case Event(QuorumCheck(clientId, requestId), state: ReplicaState) => state.handlePrepareTimeout(clientId, requestId) match {
      case(Some(clientRef), Some(msg), newstate) =>
        log.info("Failed to achieve quorum for request_id {} for client {}", requestId, clientId)
        clientRef ! msg
        stay using newstate
      case _ => stay using state  
    }
    
    case Event(msg: Commit, state: ReplicaState) => state.findRequestForCommitId(msg.commitId) match {
      case Some(rstate) =>
        log.info("Received {} message", msg)
        //TODO - Invoke Application logic 
        stay using state.handleCommit(msg.commitId, rstate.id, ClientReply(rstate.id, true))
      case None => stay using state
    }
  }
  
  //Admin/debugging messages
  whenUnhandled({
    case Event(QueryReplicaState, state: ReplicaState) =>
      sender ! state
      stay using state
  })

  def broadcastMsg(msg: VrMessage): Unit = for (i <- 1 to replicaCount) yield if(i != replicaNum) context.actorSelection("../" + i) ! msg
  
  override def preStart() = {
    if(replicaNum == 1) {
      context.system.scheduler.schedule(10 seconds, 5 seconds, self, SendCommit)
    }
  }
  
  initialize()
}

object ViewStampedReplicationDemo {

  val system = ActorSystem()

  def main(args: Array[String]): Unit = run()

  def run(): Unit = {
    // Create 5 replicas
    val replicas = for (i <- 1 to 5) yield system.actorOf(ReplicaFSM.props(5, i), String.valueOf(i))
    
    var i = 0;
    while(i < 1) {
      val future = replicas(0).ask(ClientRequest("0", i))(30 seconds)
      val result = Await.result(future, 60 seconds).asInstanceOf[ClientReply]
      system.log.info("Result[{}] => {}", result.requestId, result.success)
      i = i + 1;
    }
    //Allow for commit flush to happen.
    Thread.sleep(20000);
    
    replicas.foreach { ref =>
      val future = ref.ask(QueryReplicaState)(10 seconds)
      val result = Await.result(future, 60 seconds).asInstanceOf[ReplicaState]
      system.log.info("State for replica[{}] => {}", result.replicaNum, result)
    }
    

    system.shutdown()

  }
}