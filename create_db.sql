CREATE USER git_insights WITH LOGIN PASSWORD 'git_insights';

CREATE DATABASE git_insights WITH 
    OWNER = git_insights
    ENCODING = 'UTF8';


 CREATE TABLE commit_file_changes(
    id serial CONSTRAINT pk_commit_file_changes PRIMARY KEY,
    commit_sha VARCHAR,
    commit_parent_sha VARCHAR,
    commit_date TIMESTAMP ,
    author_timeofday INT,
    commit_email VARCHAR,
    commit_author VARCHAR,
    commit_message_length INT,
    commit_message_words INT,
    commit_lines_added INT,
    commit_lines_deleted INT,
    commit_file_count INT,
    file_change_type VARCHAR,
    file_path_old VARCHAR,
    file_path_new VARCHAR,
    file_lines_old INT,
    file_lines_new INT,
    file_lines_added INT,
    file_lines_deleted INT,
    file_hunk_count INT,
    file_hunk_min_size INT,
    file_hunk_max_size INT,
    file_hunk_avg_size FLOAT)

