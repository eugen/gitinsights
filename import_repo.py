import json
from dulwich import diff_tree
from dulwich.repo import Repo
from queue import deque
import difflib
from functools import reduce
from datetime import datetime
import re
import pprint
import csv
import sys
import psycopg2
import threading
from threading import Thread, Event
import queue

pp = pprint.PrettyPrinter(depth=6)
RENAME_SIMILARITY_THRESHOLD = 20

class FileDiffResult:
    lines_total_old = 0
    lines_total_new = 0
    lines_added = 0
    lines_deleted = 0
    hunk_count = 0
    hunk_min_size = 0
    hunk_max_size = 0
    hunk_avg_size = 0
    path_old = ""
    path_new = ""
    is_rename = False
    is_addition = False
    is_deletion = False
    def is_empty(self):
        return self.hunk_count == 0

class CommitDiffResult:
    date = 0
    file_diffs = []
    author = ""
    email = ""
    author_timeofday = 0
    sha = ""
    parent_sha = ""
    author = ""
    message = ""
    message_length = 0
    message_words = 0
    lines_added = 0
    lines_deleted = 0
    def __repr__(self):
        return "Commit:%s:%s" % (self.sha[0:8], self.email)
    

class CommitTracker:
    visited = set()
    to_visit = deque()
    def add_to_visit(self, hash):
        if hash in self.visited:
            pass
        elif hash in self.to_visit:
            pass
        else:
            self.to_visit.append(hash)

    def peek_next_to_visit(self):
        if len(self.to_visit) > 0:
            return self.to_visit[0]
        else:
            return None
            
    def pop_next_to_visit(self):
        if len(self.to_visit) > 0:
            elem = self.to_visit.popleft()
            self.visited.add(elem)
            return elem
        else:
            return None
            
    def remaining_count(self):
        return len(self.to_visit)

    def visited_count(self):
        return len(self.visited)

    def __len__(self):
        return len(self.to_visit) + len(self.visited)

def diff_files(ostore, old_file, new_file):
    result = FileDiffResult()

    old_blob_sha = old_file.sha
    new_blob_sha = new_file.sha

    # TODO: handle binary files
    old_file_lines = old_blob_sha and ostore[old_blob_sha].data.decode("utf-8").splitlines() or []
    new_file_lines = new_blob_sha and ostore[new_blob_sha].data.decode("utf-8").splitlines() or []

    result.path_old = (old_file.path or b"").decode("utf-8")
    result.path_new = (new_file.path or b"").decode("utf-8")

    if not old_blob_sha:
        result.is_addition = True
        result.change_type = "add"
    elif not new_blob_sha:
        result.is_deletion = True
        result.change_type = "del"
    elif result.path_old != result.path_new:
        result.is_rename = True
        result.change_type = "ren"
    else:
        result.change_type = "mod"

    result.lines_total_old = len(old_file_lines)
    result.lines_total_new = len(new_file_lines)

    # TODO: experiment with different values for 'n'
    # it may turn out to be statistically relevant to consider context when computing the size of a hunk
    difflines = difflib.unified_diff(old_file_lines, new_file_lines, n = 0)

    hunk_sizes = set()
    last_hunk_size = 0

    for line in difflines:
        if line[0] == '+' and not line[0:3] == "+++":
            last_hunk_size += 1
            result.lines_added += 1
        elif line[0] == '-' and not line[0:3] == "---":
            last_hunk_size += 1
            result.lines_deleted += 1
        elif line[0:2] == '@@' and last_hunk_size > 0:
            result.hunk_count += 1
            hunk_sizes.add(last_hunk_size)
            last_hunk_size = 0

    if last_hunk_size > 0:
        hunk_sizes.add(last_hunk_size)

    if len(hunk_sizes) > 0:
        result.hunk_min_size = min(hunk_sizes)
        result.hunk_max_size = max(hunk_sizes)
        result.hunk_avg_size = reduce(lambda x, y: x + y, hunk_sizes) / len(hunk_sizes)

    return result
    

def parse_commit_author(nameemail):
    match = re.match("(.*) <(.*)>", nameemail)
    if match:
        return match.groups()
    else:
        return (nameemail, "")

def diff_commits(repo, from_commit_sha, to_commit_sha):
    result = CommitDiffResult()

    ostore = repo.object_store
    to_commit = ostore[to_commit_sha]
    timeofday = datetime.fromtimestamp(to_commit.author_time).time()
    message = to_commit.message.decode('utf-8')
    nameemail = parse_commit_author(to_commit.author.decode("utf-8"))

    result.sha = to_commit_sha.decode("ascii")
    result.date = to_commit.commit_time
    result.parent_sha = from_commit_sha.decode("ascii")
    (result.author, result.email) = nameemail
    result.author_timeofday = timeofday.hour*24 + timeofday.minute
    result.message = message
    result.message_words = len(re.split('\\s+', message))

    changes = diff_tree.tree_changes(
        ostore, 
        ostore[from_commit_sha].tree, 
        to_commit.tree,
        rename_detector = diff_tree.RenameDetector(ostore, RENAME_SIMILARITY_THRESHOLD, 1000, None, False))

    for change in changes:
        try:
            diff_result = diff_files(ostore, change.old, change.new)
            if not diff_result.is_empty():
                result.file_diffs.append(diff_result)
        except Exception as e:
            # move along, sir, nothing to see here
            print('something went wrong', e)
            continue

    result.lines_added = sum([fd.lines_added for fd in result.file_diffs])
    result.lines_deleted = sum([fd.lines_deleted for fd in result.file_diffs])

    return result

def walk_repo(repo):
    commit_tracker = CommitTracker()

    for ref_key in repo.refs.keys():
        ref = repo.refs[ref_key]
        if repo.object_store[ref].type_name != b'commit':
            print("can't handle %s objects, skipping" % repo.object_store[ref].type_name.decode("ascii"))
            continue

        commit_tracker.add_to_visit(ref)
    
        processed_counter = 0
        while commit_tracker.remaining_count() > 0:
            commit_hash = commit_tracker.pop_next_to_visit()
            processed_counter += 1
            if processed_counter % 100 == 0:
                print("processed %d commits" % processed_counter)
            commit = repo.object_store[commit_hash]
            # TODO: handle merge commits better
            for parent in commit.parents:
                # add all parents so that we continue to walk the repo
                commit_tracker.add_to_visit(parent)
            if len(commit.parents) == 1:
                # but only yield diffs for single-parent commits
                yield diff_commits(repo, parent, commit_hash)



def scan_repo_to_csv(repo_path, csv_path):
    repo = Repo.discover(repo_path)
    with open(csv_path, "w+") as stream:
        writer = csv.writer(stream, lineterminator = "\n")
        writer.writerow(["date", "sha", "parent", "message", "author", "email", "author_time_mins", "message_length", "message_words", "commit_lines_added", "commit_lines_deleted", "changed_files", "change_type", "path_old", "path_new", "lines_total_old", "lines_total_new", "lines_added", "lines_deleted", "hunk_count", "hunk_min_size", "hunk_max_size", "hunk_avg_size"])
        for commit in walk_repo(repo):
            for file in commit.file_diffs:
                writer.writerow([
                    commit.date,
                    commit.sha,
                    commit.parent_sha,
                    commit.message[0:20],
                    commit.author,
                    commit.email,
                    commit.author_timeofday,
                    commit.message_length,
                    commit.message_words,
                    commit.lines_added,
                    commit.lines_deleted,
                    len(commit.file_diffs),
                    file.change_type,
                    file.path_old,
                    file.path_new,
                    file.lines_total_old,
                    file.lines_total_new,
                    file.lines_added,
                    file.lines_deleted,
                    file.hunk_count,
                    file.hunk_min_size,
                    file.hunk_max_size,
                    "%.2f" % file.hunk_avg_size
                ])

class PsqlStatementParallelizer:
    """This must be called with with.."""
    parallelism = 4
    threads = []
    commits = None
    stopped_event = Event()
    connection_string = None

    def __init__(self, connection_string, threads = 4):
        myself = self
        self.parallelism = threads
        self.connection_string = connection_string
        for i in range(0, self.parallelism):
            self.threads.append(Thread(target = self.pop_and_insert_ad_infinitum))
    
    def __enter__(self):
        self.commits = queue.Queue()
        for t in self.threads:
            t.start()
        return self

    def __exit__(self, type, value, traceback):
        # all threads must die
        self.stopped_event.set()
        for t in self.threads:
            t.join()

    def stop(self):
        self.stopped_event.set()

    def __insert_commit__(self, commit, cursor):
        for file in commit.file_diffs:
            cursor.execute("""
                INSERT INTO commit_file_changes(
                    commit_sha,
                    commit_parent_sha,
                    commit_date,
                    author_timeofday,
                    commit_email,
                    commit_author,
                    commit_message_length,
                    commit_message_words,
                    commit_lines_added,
                    commit_lines_deleted,
                    commit_file_count,
                    file_change_type,
                    file_path_old,
                    file_path_new,
                    file_lines_old,
                    file_lines_new,
                    file_lines_added,
                    file_lines_deleted,
                    file_hunk_count,
                    file_hunk_min_size,
                    file_hunk_max_size,
                    file_hunk_avg_size)
                VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                commit.sha,
                commit.parent_sha,
                datetime.fromtimestamp(commit.date),
                commit.author_timeofday,
                commit.email, 
                commit.author,
                commit.message_length,
                commit.message_words,
                commit.lines_added,
                commit.lines_deleted,
                len(commit.file_diffs),
                file.change_type,
                file.path_old,
                file.path_new,
                file.lines_total_old,
                file.lines_total_new,
                file.lines_added,
                file.lines_deleted,
                file.hunk_count,
                file.hunk_min_size,
                file.hunk_max_size,
                file.hunk_avg_size
            ));
                        

    def pop_and_insert_ad_infinitum(self):
        print("self is this:", self)
        with psycopg2.connect(self.connection_string) as conn:
            with conn.cursor() as curs:
                while not self.is_stopped():
                    commit_batch = []
                    try:
                        commit_batch.append(self.commits.get(timeout = 1))
                    except queue.Empty:
                        continue
        
                    # try to read 100 items, but give up whenever we can't read an item in 5ms
                    while len(commit_batch) < 100:
                        try:
                            commit_batch.append(self.commits.get(timeout = 0.5))
                            #print("got some more commits, batch size is now %d" % len(commit_batch))
                        except queue.Empty:
                            #print("done waiting for new element, all I got is a batch of %d" % len(commit_batch))
                            break
        
                    if len(commit_batch) == 0:
                        continue
        
                    # we've got a batch! let's execute it
                    for commit in commit_batch:
                        try: 
                            self.__insert_commit__(commit, curs)
                        except BaseException as e:
                            print("Exception while inserting commit", e)
                            quit()
                            
                    print("Committing txn for %d commits from thread %s" % (len(commit_batch), threading.current_thread().name))
                    try:
                        conn.commit()
                    except BaseException as e:
                        print("Exception while committing batch", e)
                        quit()
                
                print("Thread %s detected event to stop" % threading.current_thread().name)

                
    def add_commit(self, commit):
        if not self.commits:
            raise(Exception("Cannot use " + self.__class__.__name__ + " outside of an with() statement"))
        #print("adding a commit to the queue of ~size %d" % self.commits.qsize())
        self.commits.put(commit)        
        
    def is_stopped(self):
        return self.stopped_event.is_set()


def scan_repo_to_postgresql(repo_path, psql_conn_string):
    repo = Repo.discover(repo_path)
    with PsqlStatementParallelizer(psql_conn_string) as para:
        try:
            for commit in walk_repo(repo):
                para.add_commit(commit)
        finally:
            pass
            #para.stop()



#scan_repo_to_csv("D:/src/spring-framework/", "d:/tmp/spring-framework.csv")
#scan_repo_to_postgresql("D:/src/spring-boot/", "dbname=git_insights user=git_insights password=git_insights")
scan_repo_to_postgresql("D:/src/spring-boot/", "dbname=git_insights user=git_insights password=git_insights")
