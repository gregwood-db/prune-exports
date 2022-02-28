import json
import os
import argparse
import shutil


def prune_all_resources(tags, src_path, dst_path, overwrite):

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
        os.makedirs(os.path.join(dst_path, "groups"))

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
    print("Pruning workspace metadata (this may take several minutes)...")
    prune_workspace_metadata(tags, users_to_keep, src_path, dst_path, overwrite)

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
    safe_copy(os.path.join(src_path, "metastore"),
              os.path.join(dst_path, "metastore"), overwrite)
    safe_copy(os.path.join(src_path, "database_details.log"),
              os.path.join(dst_path, "database_details.log"), overwrite)

    print("Finished pruning resources.")
    return 0


def safe_copy(src, dst, overwrite=False):
    """Copies a file or directory with overwrite protection."""
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

    if os.path.isfile(dst_cluster_file) and not overwrite:
        print("Found existing clusters.log; skipping pruning...")
        copied_clusters = []
        with open(dst_cluster_file, 'r') as f:
            for line in f:
                cluster = json.loads(line)
                copied_clusters.append(cluster["cluster_id"])
    else:
        copied_clusters = []
        with open(src_cluster_file, 'r') as src, open(dst_cluster_file, 'w') as dst:
            for line in src:
                cluster = json.loads(line)

                # make sure cluster has custom tags defined
                if "custom_tags" not in cluster.keys():
                    continue

                # make sure cluster has the "z_team_tag" defined; if defined, check if it equals the provided tag(s)
                cluster_tags = cluster["custom_tags"]
                if "z_team" not in cluster_tags.keys():
                    continue
                # check if the team is in our list of tags; if so, write the line and record the cluster ID
                elif cluster_tags["z_team"] in tags:
                    dst.write(line)
                    copied_clusters.append(cluster["cluster_id"])

    # copy appropriate cluster ACLs to destination
    if os.path.isfile(dst_cluster_file) and not overwrite:
        print("Found existing acl_clusters.log; skipping pruning...")
    else:
        with open(src_cluster_acls, 'r') as src, open(dst_cluster_acls, 'w') as dst:
            for line in src:
                cluster = json.loads(line)["object_id"].split("/clusters/")[1]
                if cluster in copied_clusters:
                    dst.write(line)

    return copied_clusters


def prune_jobs(tags, clusters, src_path, dst_path, overwrite):
    """Copies jobs to the dest export path if they correspond to a valid cluster, or contain the provided tag(s)"""
    src_job_file = os.path.join(src_path, "jobs.log")
    dst_job_file = os.path.join(dst_path, "jobs.log")
    src_job_acls = os.path.join(src_path, "acl_jobs.log")
    dst_job_acls = os.path.join(dst_path, "acl_jobs.log")

    if os.path.isfile(dst_job_file) and not overwrite:
        print("Found existing jobs.log; skipping pruning...")
    else:
        job_ids = []
        with open(src_job_file, 'r') as src, open(dst_job_file, 'w') as dst:
            for line in src:
                job = json.loads(line)
                job_keys = job["settings"].keys()
                if "custom_tags" in job_keys:
                    job_tags = job["settings"]["custom_tags"]
                    if "z_team" not in job_tags:
                        continue
                    elif job_tags["z_team"] in tags:
                        dst.write(line)
                        job_ids.append(job["job_id"])
                elif "existing_cluster_id" in job_keys:
                    if job["settings"]["existing_cluster_id"] in clusters:
                        dst.write(line)
                        job_ids.append(job["job_id"])

    # copy job ACLs
    if os.path.isfile(dst_job_acls) and not overwrite:
        print("Found existing acl_jobs.log; skipping pruning...")
    else:
        job_ids = []
        with open(dst_job_file, 'r') as f:
            for line in f:
                job = json.loads(line)
                job_ids.append(job["job_id"])

        with open(src_job_acls, 'r') as src, open(dst_job_acls, 'w') as dst:
            for line in src:
                obj_id = json.loads(line)["object_id"].split("/jobs/")[1]
                if int(obj_id) in job_ids:
                    dst.write(line)


def prune_instance_profiles(tags, src_path, dst_path, overwrite):
    """Writes instance profiles in the source that are matched against the tag list to the destination logfile."""
    src_ip_file = os.path.join(src_path, "instance_profiles.log")
    dst_ip_file = os.path.join(dst_path, "instance_profiles.log")

    if os.path.isfile(dst_ip_file) and not overwrite:
        print("Found existing instance_profiles.log; skipping pruning...")
        return

    with open(src_ip_file, 'r') as src, open(dst_ip_file, 'w') as dst:
        for line in src:
            profile = json.loads(line)
            arn = profile["instance_profile_arn"]

            # if the tag exists in the ARN, add it to our target profiles
            # note that tags may include _ but ARNs cannot, so replace them with -
            if [x for x in tags if x.replace("_", "-") in arn]:
                dst.write(line)


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

    if os.path.isfile(dst_users_file) and not overwrite:
        print("Found existing users.log; skipping pruning...")
        return

    with open(src_users_file, 'r') as src, open(dst_users_file, 'w') as dst:
        for line in src:
            username = json.loads(line)["userName"]
            if username in users_to_keep:
                dst.write(line)


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

    if os.path.isfile(dst_users_dirs) and not overwrite:
        print("Found existing user_dirs.log; skipping pruning...")
        do_copy = False
    else:
        do_copy = True

    # first step through directories to copy appropriate objects
    copied_dirs = []
    dir_ids = []
    with open(src_users_dirs, 'r') as src, open(dst_users_dirs, 'w') as dst:
        for line in src:
            dir_name = json.loads(line)["path"]
            obj_id = json.loads(line)["object_id"]
            # copy any top-level directories
            if len(dir_name.split("/")) == 2:
                if do_copy:
                    dst.write(line)
            # copy User directories where usernames match
            elif dir_name.split("/")[1] == "Users":
                user = dir_name.split("/")[2]
                if user in users_to_keep:
                    if do_copy:
                        dst.write(line)
                    copied_dirs.append(dir_name)
                    dir_ids.append(obj_id)
            # copy team directories where team name matches tag
            elif dir_name.split("/")[1] == "teams":
                team = dir_name.split("/")[2]
                # sometimes directory is /team/team_name and sometimes /team/name
                if [x for x in tags if x.replace("_", "-") in team] or\
                        [x for x in tags if x.split("team_")[1] in team]:
                    if do_copy:
                        dst.write(line)
                    copied_dirs.append(dir_name)
                    dir_ids.append(obj_id)

    # next step through object metadata to copy appropriate items
    if os.path.isfile(dst_users_ws) and not overwrite:
        print("Found existing user_workspace.log; skipping pruning...")
        do_copy = False
    else:
        do_copy = True

    file_ids = []
    with open(src_users_ws, 'r') as src, open(dst_users_ws, 'w') as dst:
        for line in src:
            file_name = json.loads(line)['path']
            file_id = json.loads(line)['object_id']
            # copy all files in directories copied above
            if [x for x in copied_dirs if x in file_name]:
                if do_copy:
                    dst.write(line)
                file_ids.append(file_id)

    # copy directory ACLs
    if os.path.isfile(dst_dir_acls) and not overwrite:
        print("Found existing acl_directories.log; skipping pruning...")
    else:
        with open(src_dir_acls, 'r') as src, open(dst_dir_acls, 'w') as dst:
            for line in src:
                obj_id = json.loads(line)["object_id"].split("/directories/")[1]
                if int(obj_id) in dir_ids:
                    dst.write(line)

    # copy object ACLs
    if os.path.isfile(dst_obj_acls) and not overwrite:
        print("Found existing acl_notebooks.log; skipping pruning...")
    else:
        with open(src_obj_acls, 'r') as src, open(dst_obj_acls, 'w') as dst:
            for line in src:
                obj_id = json.loads(line)["object_id"].split("/notebooks/")[1]
                if int(obj_id) in file_ids:
                    dst.write(line)

    # copy workspace libraries
    if os.path.isfile(dst_libraries) and not overwrite:
        print("Found existing libraries.log; skipping pruning...")
    else:
        with open(src_libraries, 'r') as src, open(dst_libraries, 'w') as dst:
            for line in src:
                file_name = json.loads(line)['path']
                # copy all files in directories copied above
                if [x for x in copied_dirs if x in file_name]:
                    dst.write(line)


def get_parser():
    parser = argparse.ArgumentParser(description='Prune exported workspace resources using tags')

    parser.add_argument("--source-path", action="store",
                        help="The folder containing the exported resources to be pruned.")

    parser.add_argument("--target-path", action="store",
                        help="The folder to write the pruned artifacts.")

    parser.add_argument("--tags", action="store", nargs="+",
                        help="The tag(s) defining which clusters and resources to keep.")

    parser.add_argument("--overwrite", action="store_true",
                        help="If specified, overwrites the target folder.")

    parser.add_argument('--specs', action="store",
                        help="A tab-delimited file specifying additional resources to prune.")

    return parser


def main():
    args = get_parser().parse_args()
    prune_all_resources(args.tags, args.source_path, args.target_path, args.overwrite)


if __name__ == "__main__":
    main()
