----------------------------- MODULE RaftKV -----------------------------
EXTENDS Naturals, FiniteSets, Sequences

CONSTANTS Server, MaxTerm, Nil

VARIABLES
    currentTerm, \* [Server -> Nat]
    state,       \* [Server -> {"Follower", "Candidate", "Leader"}]
    votedFor,    \* [Server -> Server \cup {Nil}]
    log,         \* [Server -> Seq([term: Nat])]
    votesGranted \* [Server -> SUBSET Server]

vars == <<currentTerm, state, votedFor, log, votesGranted>>

Quorum == {Q \in SUBSET Server : Cardinality(Q) * 2 > Cardinality(Server)}

LastTerm(s) == IF Len(log[s]) = 0 THEN 0 ELSE log[s][Len(log[s])].term

UpToDate(cand, voter) ==
    \/ LastTerm(cand) > LastTerm(voter)
    \/ (LastTerm(cand) = LastTerm(voter) /\ Len(log[cand]) >= Len(log[voter]))

Init ==
    /\ currentTerm = [s \in Server |-> 0]
    /\ state = [s \in Server |-> "Follower"]
    /\ votedFor = [s \in Server |-> Nil]
    /\ log = [s \in Server |-> <<>>]
    /\ votesGranted = [s \in Server |-> {}]

Timeout(s) ==
    /\ state[s] \in {"Follower", "Candidate"}
    /\ currentTerm[s] < MaxTerm
    /\ currentTerm' = [currentTerm EXCEPT ![s] = @ + 1]
    /\ state' = [state EXCEPT ![s] = "Candidate"]
    /\ votedFor' = [votedFor EXCEPT ![s] = s]
    /\ votesGranted' = [votesGranted EXCEPT ![s] = {s}]
    /\ UNCHANGED <<log>>

GrantVote(voter, cand) ==
    /\ state[cand] = "Candidate"
    /\ currentTerm[cand] >= currentTerm[voter]
    /\ (currentTerm[cand] > currentTerm[voter] \/ votedFor[voter] \in {Nil, cand})
    /\ UpToDate(cand, voter)
    /\ currentTerm' = [currentTerm EXCEPT ![voter] = currentTerm[cand]]
    /\ state' = [state EXCEPT ![voter] = "Follower"]
    /\ votedFor' = [votedFor EXCEPT ![voter] = cand]
    /\ votesGranted' = [votesGranted EXCEPT ![cand] = @ \cup {voter}]
    /\ UNCHANGED <<log>>

BecomeLeader(s) ==
    /\ state[s] = "Candidate"
    /\ votesGranted[s] \in Quorum
    /\ state' = [state EXCEPT ![s] = "Leader"]
    /\ UNCHANGED <<currentTerm, votedFor, log, votesGranted>>

ClientAppend(s) ==
    /\ state[s] = "Leader"
    /\ Len(log[s]) < 3
    /\ log' = [log EXCEPT ![s] = Append(@, [term |-> currentTerm[s]])]
    /\ UNCHANGED <<currentTerm, state, votedFor, votesGranted>>

Next ==
    \/ \E s \in Server : Timeout(s) \/ BecomeLeader(s) \/ ClientAppend(s)
    \/ \E v, c \in Server : GrantVote(v, c)

Spec == Init /\ [][Next]_vars

\* Core Safety Invariant 1: At most one leader can be elected in any given term.
ElectionSafety ==
    \A s1, s2 \in Server :
        (state[s1] = "Leader" /\ state[s2] = "Leader" /\ currentTerm[s1] = currentTerm[s2])
        => (s1 = s2)

=============================================================================
