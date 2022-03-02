import json
import os
import argparse
import shutil
import pandas as pd


def prune_all_resources(tags, src_path, dst_path, overwrite, metastore_flag):
    # check if source folder exists
    if not os.path.isdir(src_path):
        print("Error: could not find source path.")
        return -1

    # check if destination folder already exists
    print("Checking for existing destination folder...")
    if os.path.isdir(dst_path):
        if overwrite:
            print("Existing destination path found; overwriting existing files.")
        else:
            print("Existing destination path found; will skip existing files.")
    else:
        print("Destination path not found. Creating...")
        os.makedirs(dst_path)

    # prune clusters & jobs
    print("Pruning clusters...")
    clusters_to_keep = prune_clusters(tags, src_path, dst_path, overwrite)
    print("Pruning jobs...")
    prune_jobs(tags, clusters_to_keep, src_path, dst_path, overwrite)

    # prune instance profiles
    print("Pruning instance profiles...")
    prune_instance_profiles(tags, src_path, dst_path, overwrite)

    # prune groups & users
    print("Pruning groups...")
    users_to_keep = prune_groups(tags, src_path, dst_path, overwrite)
    print("Pruning users...")
    prune_users(users_to_keep, src_path, dst_path, overwrite)

    # prune workspace metadata
    print("Pruning workspace metadata...")
    prune_workspace_metadata(tags, users_to_keep, src_path, dst_path, overwrite)

    # prune workspace objects
    print("Pruning workspace artifacts...")
    prune_artifacts(tags, users_to_keep, src_path, dst_path, overwrite)

    # copy other resources from source to dest
    print("Copying additional resources to new export path...")
    safe_copy(os.path.join(src_path, "instance_pools.log"),
              os.path.join(dst_path, "instance_pools.log"), overwrite)
    safe_copy(os.path.join(src_path, "cluster_policies.log"),
              os.path.join(dst_path, "cluster_policies.log"), overwrite)
    safe_copy(os.path.join(src_path, "acl_cluster_policies.log"),
              os.path.join(dst_path, "acl_cluster_policies.log"), overwrite)
    safe_copy(os.path.join(src_path, "table_acls"),
              os.path.join(dst_path, "table_acls"), overwrite)
    safe_copy(os.path.join(src_path, "database_details.log"),
              os.path.join(dst_path, "database_details.log"), overwrite)
    safe_copy(os.path.join(src_path, "secret_scopes"),
              os.path.join(dst_path, "secret_scopes"), overwrite)
    safe_copy(os.path.join(src_path, "secret_scopes_acls.log"),
              os.path.join(dst_path, "secret_scopes_acls.log"), overwrite)
    safe_copy(os.path.join(src_path, "source_info.txt"),
              os.path.join(dst_path, "source_info.txt"), overwrite)
    safe_copy(os.path.join(src_path, "cluster_ids_to_change_creator.log"),
              os.path.join(dst_path, "cluster_ids_to_change_creator.log"), overwrite)
    safe_copy(os.path.join(src_path, "original_creator_user_ids.log"),
              os.path.join(dst_path, "original_creator_user_ids.log"), overwrite)
    safe_copy(os.path.join(src_path, "user_name_to_user_id.log"),
              os.path.join(dst_path, "user_name_to_user_id.log"), overwrite)

    if not metastore_flag:
        safe_copy(os.path.join(src_path, "metastore"),
                  os.path.join(dst_path, "metastore"), overwrite)
        safe_copy(os.path.join(src_path, "metastore_views"),
                  os.path.join(dst_path, "metastore_views"), overwrite)

    print("Finished pruning resources.")
    return 0


def write_multiline_df(df, file):
    """Writes a multi-line DataFrame to a json file without escaping slashes (as Pandas does)"""
    df_json = json.loads(df.to_json(orient="records"))

    with open(file, 'w') as f:
        for item in df_json:
            f.write(json.dumps(item) + "\n")


def safe_copy(src, dst, overwrite=False):
    """Copies a file or directory with overwrite protection."""
    if not os.path.isfile(src) and not os.path.isdir(src):
        print("Warning: could not find source file or directory {}; skipping copy.".format(src))
    if overwrite:
        if os.path.isfile(src):
            shutil.copy2(src, dst)
        elif os.path.isdir(src):
            if os.path.exists(dst):
                shutil.rmtree(dst)
            shutil.copytree(src, dst)
    else:
        if not os.path.isfile(dst) and not os.path.isdir(dst):
            if os.path.isfile(src):
                shutil.copy2(src, dst)
            elif os.path.isdir(src):
                shutil.copytree(src, dst)
        else:
            print("Warning: file or directory {} exists; skipping copy.".format(dst))


def prune_clusters(tags, src_path, dst_path, overwrite):
    """Writes clusters that have a z_team tag existing in the input tags list to the destination logfile."""
    src_cluster_file = os.path.join(src_path, "clusters.log")
    dst_cluster_file = os.path.join(dst_path, "clusters.log")
    src_cluster_acls = os.path.join(src_path, "acl_clusters.log")
    dst_cluster_acls = os.path.join(dst_path, "acl_clusters.log")

    if not os.path.isfile(src_cluster_file):
        print("Warning: no clusters.log file found, skipping Cluster pruning...")
        return []
    elif os.path.isfile(dst_cluster_file) and not overwrite:
        print("Found existing clusters.log; skipping pruning...")
        clusters_df = pd.read_json(dst_cluster_file, lines=True)
    else:
        clusters_df = pd.read_json(src_cluster_file, lines=True)
        clusters_df = clusters_df[clusters_df.custom_tags.apply(pd.Series).z_team.isin(tags)]
        write_multiline_df(clusters_df, dst_cluster_file)

    copied_clusters = list(clusters_df.cluster_id)

    # copy cluster ACLs to destination if required
    if os.path.isfile(dst_cluster_acls) and not overwrite:
        print("Found existing acl_clusters.log; skipping pruning...")
    else:
        cluster_acls_df = pd.read_json(src_cluster_acls, lines=True)
        cluster_acls_df = cluster_acls_df[cluster_acls_df.object_id.str.split("/").str[2].isin(copied_clusters)]
        write_multiline_df(cluster_acls_df, dst_cluster_acls)

    return copied_clusters


def prune_jobs(tags, clusters, src_path, dst_path, overwrite):
    """Copies jobs to the dest export path if they correspond to a valid cluster, or contain the provided tag(s)"""
    src_job_file = os.path.join(src_path, "jobs.log")
    dst_job_file = os.path.join(dst_path, "jobs.log")
    src_job_acls = os.path.join(src_path, "acl_jobs.log")
    dst_job_acls = os.path.join(dst_path, "acl_jobs.log")

    if not os.path.isfile(src_job_file):
        print("Warning: no jobs.log file found, skipping Job pruning...")
        return
    elif os.path.isfile(dst_job_file) and not overwrite:
        print("Found existing jobs.log; skipping pruning...")
        jobs_df = pd.read_json(dst_job_file, lines=True)
    else:
        jobs_df = pd.read_json(src_job_file, lines=True)
        jobs_existing = jobs_df[jobs_df.settings.apply(pd.Series).existing_cluster_id.isin(clusters)]
        jobs_tags = jobs_df.settings.apply(pd.Series).new_cluster.apply(pd.Series).get("custom_tags")
        if jobs_tags is not None:
            jobs_new = jobs_df[jobs_tags.apply(pd.Series).z_team.isin(tags)]
            jobs_df = pd.concat([jobs_existing, jobs_new])
        else:
            jobs_df = jobs_existing

        write_multiline_df(jobs_df, dst_job_file)

    # copy job ACLs
    if os.path.isfile(dst_job_acls) and not overwrite:
        print("Found existing acl_jobs.log; skipping pruning...")
    else:
        job_ids = [str(x) for x in list(jobs_df.job_id)]
        job_acls_df = pd.read_json(src_job_acls, lines=True)
        job_acls_df = job_acls_df[job_acls_df.object_id.str.split("/").str[2].isin(job_ids)]
        write_multiline_df(job_acls_df, dst_job_acls)


def prune_instance_profiles(tags, src_path, dst_path, overwrite):
    """Writes instance profiles in the source that are matched against the tag list to the destination logfile."""
    src_ip_file = os.path.join(src_path, "instance_profiles.log")
    dst_ip_file = os.path.join(dst_path, "instance_profiles.log")

    if not os.path.isfile(src_ip_file):
        print("Warning: no instance_profiles.log file found, skipping Profile pruning...")
        return
    elif os.path.isfile(dst_ip_file) and not overwrite:
        print("Found existing instance_profiles.log; skipping pruning...")
        return

    ip_df = pd.read_json(src_ip_file, lines=True)
    ip_df = ip_df[ip_df.instance_profile_arn
                  .apply(lambda x: any([k in x for k in [t.replace("_", "-") for t in tags]]))]
    write_multiline_df(ip_df, dst_ip_file)


def prune_groups(tags, src_path, dst_path, overwrite):
    """Writes groups that match the input tags list to the destination folder, and returns all nested users."""
    src_groups_dir = os.path.join(src_path, "groups")
    dst_groups_dir = os.path.join(dst_path, "groups")

    if os.path.isdir(dst_groups_dir) and not overwrite:
        print("Groups directory exists; skipping pruning...")
        target_group_dir = dst_groups_dir
        do_copy = False
    else:
        if not os.path.isdir(dst_groups_dir):
            os.makedirs(dst_groups_dir)
        target_group_dir = src_groups_dir
        do_copy = True

    group_list = list(os.walk(target_group_dir))[0][2]
    users_to_keep = []
    for group in group_list:
        # check if the group belongs to our migration list
        if [x for x in tags if x.replace("_", "-") in group]:
            # copy the group to the destination
            if do_copy:
                safe_copy(os.path.join(src_groups_dir, group), os.path.join(dst_groups_dir, group), True)
            # pull out users that belong to this group
            with open(os.path.join(src_groups_dir, group), 'r') as f:
                group_users = json.load(f)
            # add users to the return list
            for user in group_users["members"]:
                users_to_keep.append(user["userName"])

    return users_to_keep


def prune_users(users_to_keep, src_path, dst_path, overwrite):
    """Writes users that exist in the input list to the destination users file."""
    src_users_file = os.path.join(src_path, "users.log")
    dst_users_file = os.path.join(dst_path, "users.log")

    if not os.path.isfile(src_users_file):
        print("Warning: no users.log file found, skipping Users pruning...")
        return
    elif os.path.isfile(dst_users_file) and not overwrite:
        print("Found existing users.log; skipping pruning...")
        return

    users_df = pd.read_json(src_users_file, lines=True)
    users_df.id = users_df.id.astype(str)
    users_df = users_df[users_df.userName.isin(users_to_keep)]
    write_multiline_df(users_df, dst_users_file)


def prune_workspace_metadata(tags, users_to_keep, src_path, dst_path, overwrite):
    """Copies tagged workspace object metadata from src to dir, including libraries"""
    src_users_dirs = os.path.join(src_path, "user_dirs.log")
    dst_users_dirs = os.path.join(dst_path, "user_dirs.log")
    src_users_ws = os.path.join(src_path, "user_workspace.log")
    dst_users_ws = os.path.join(dst_path, "user_workspace.log")
    src_dir_acls = os.path.join(src_path, "acl_directories.log")
    dst_dir_acls = os.path.join(dst_path, "acl_directories.log")
    src_obj_acls = os.path.join(src_path, "acl_notebooks.log")
    dst_obj_acls = os.path.join(dst_path, "acl_notebooks.log")
    src_libraries = os.path.join(src_path, "libraries.log")
    dst_libraries = os.path.join(dst_path, "libraries.log")

    # check if we can skip all pruning of metadata
    if not overwrite and \
            (os.path.isfile(dst_users_dirs) and os.path.isfile(dst_users_dirs) and
             os.path.isfile(dst_users_ws) and os.path.isfile(dst_dir_acls) and
             os.path.isfile(dst_obj_acls) and os.path.isfile(dst_libraries)):
        print("Skipping workspace metadata pruning...")
        return

    # break out directories by users and teams
    user_dir_df = pd.read_json(src_users_dirs, lines=True)
    top_level_dirs = user_dir_df[user_dir_df.path.str.split("/").str.len() == 2]
    user_dirs = user_dir_df[user_dir_df.path.str.split("/", expand=True)[1] == "Users"]
    team_dirs = user_dir_df[user_dir_df.path.str.split("/", expand=True)[1] == "teams"]

    # filter user and team directories
    user_dirs = user_dirs[user_dirs.path.str.split("/", expand=True)[2].isin(users_to_keep)]
    tags_short = [t.split("_")[1] for t in tags]
    team_dirs = team_dirs[team_dirs.path.str.split("/", expand=True)[2].isin(tags_short + tags)]
    all_dirs = pd.concat([user_dirs, team_dirs])
    copied_dirs = list(all_dirs.path)
    dir_ids = [str(d) for d in list(all_dirs.object_id)]

    # write file if it doesn't exist
    if os.path.isfile(dst_users_dirs) and not overwrite:
        print("Found existing user_dirs.log; skipping pruning...")
    else:
        write_multiline_df(pd.concat([top_level_dirs, all_dirs]), dst_users_dirs)

    # get all workspace objects
    workspace_df = pd.read_json(src_users_ws, lines=True)

    # filter per directories copied above
    workspace_df = workspace_df[workspace_df.path.str.split("/").str[:-1].str.join("/").isin(copied_dirs)]
    file_ids = [str(f) for f in list(workspace_df.object_id)]

    # write object if it doesn't exist
    if os.path.isfile(dst_users_ws) and not overwrite:
        print("Found existing user_workspace.log; skipping pruning...")
    else:
        write_multiline_df(workspace_df, dst_users_ws)

    # copy directory ACLs
    if os.path.isfile(dst_dir_acls) and not overwrite:
        print("Found existing acl_directories.log; skipping pruning...")
    else:
        dir_acls_df = pd.read_json(src_dir_acls, lines=True)
        dir_acls_df = dir_acls_df[dir_acls_df.object_id.str.split("/", expand=True)[2].isin(dir_ids)]
        write_multiline_df(dir_acls_df, dst_dir_acls)

    # copy object ACLs
    if os.path.isfile(dst_obj_acls) and not overwrite:
        print("Found existing acl_notebooks.log; skipping pruning...")
    else:
        obj_acls_df = pd.read_json(src_obj_acls, lines=True)
        obj_acls_df = obj_acls_df[obj_acls_df.object_id.str.split("/", expand=True)[2].isin(file_ids)]
        write_multiline_df(obj_acls_df, dst_obj_acls)

    # copy workspace libraries
    if not os.path.isfile(src_libraries):
        print("Warning: no libraries.log file found, skipping Library pruning...")
    if os.path.isfile(dst_libraries) and not overwrite:
        print("Found existing libraries.log; skipping pruning...")
    else:
        library_df = pd.read_json(src_libraries, lines=True)
        if len(library_df) > 0:
            library_df = library_df[library_df.path.str.split("/").str[:-1].str.join("/").isin(copied_dirs)]
            write_multiline_df(library_df, dst_libraries)


def prune_artifacts(tags, users_to_keep, src_path, dst_path, overwrite):
    """Copies workspace artifacts of pruned users and teams."""
    src_obj_dir = os.path.join(src_path, "artifacts")
    dst_obj_dir = os.path.join(dst_path, "artifacts")

    if not os.path.isdir(dst_obj_dir):
        os.makedirs(dst_obj_dir)

    team_dir = os.path.join(src_obj_dir, "teams")
    if os.path.isdir(team_dir):
        dir_list = list(os.walk(team_dir))[0][1]
        for d in dir_list:
            if d in [x.split("_")[1] for x in tags]:
                src = os.path.join(team_dir, d)
                dst = os.path.join(dst_obj_dir, "teams", d)
                safe_copy(src, dst, overwrite)

    user_dir = os.path.join(src_obj_dir, "Users")
    if os.path.isdir(team_dir):
        dir_list = list(os.walk(user_dir))[0][1]
        for d in dir_list:
            if d in users_to_keep:
                src = os.path.join(user_dir, d)
                dst = os.path.join(dst_obj_dir, "Users", d)
                safe_copy(src, dst, overwrite)


def get_parser():
    parser = argparse.ArgumentParser(description='Prune exported workspace resources using tags')

    parser.add_argument("--source", action="store",
                        help="The folder containing the exported resources to be pruned.")

    parser.add_argument("--target", action="store",
                        help="The folder to write the pruned artifacts.")

    parser.add_argument("--tags", action="store", nargs="+",
                        help="The tag(s) defining which clusters and resources to keep.")

    parser.add_argument("--overwrite", action="store_true",
                        help="If specified, overwrites the target folder.")

    parser.add_argument("--skip-metastore", action="store_true",
                        help="If specified, skip writing the metastore.")

    parser.add_argument("--skip-artifacts", action="store_true",
                        help="If specified, skip writing the workspace artifacts.")

    return parser


def main():
    args = get_parser().parse_args()
    prune_all_resources(args.tags, args.source, args.target, args.overwrite, args.skip_metastore)


if __name__ == "__main__":
    main()
