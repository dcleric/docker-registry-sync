import Queue
import argparse
import itertools
import threading
import time

import docker
import requests
from docker import errors

validate_queue = Queue.Queue(maxsize=0)
good_image_queue = Queue.Queue(maxsize=0)


def get_docker_registry_list(registry_prefix):
    registry_url = 'https://' + registry_prefix
    catalog = requests.get(url=registry_url + '/v2/_catalog?n=10000')
    repo_list = catalog.json().get('repositories')
    tags_list = []
    fulltags_list = []
    if catalog.status_code == 200 or 'errors' not in catalog.json():
        for repo in repo_list:
            try:
                tags = requests.get(
                    url=registry_url + '/v2/%s/tags/list' % repo)
                tags_list.append(tags.json())
            except Exception as e:
                print ('Error: {}', e)
    else:
        raise docker.errors.APIError

    for tag_entry in tags_list:
        if 'errors' not in tag_entry:
            repo_name = tag_entry.get('name')
            for tag in tag_entry.get('tags'):
                repo_dict = dict()
                repo_dict['name'] = repo_name
                repo_dict['tag'] = tag
                fulltags_list.append(repo_dict)

    return fulltags_list


def validate_registry_list():
    while True:
        try:
            image_entry = validate_queue.get(timeout=5)
        except Queue.Empty:
            print 'manifest validate queue is empty'
            break
        valid_entry = (requests.get(url='https://' + source_registry +
                                        '/v2/%s/manifests/%s' % (image_entry.get('name'), image_entry.get('tag'))))
        if valid_entry.status_code == 200:
            good_image_queue.put(image_entry)

    print 'validate queue size', validate_queue.qsize()


def get_diff_list(list1, list2):
    diff_list = list(itertools.ifilterfalse(lambda x: x in list2, list1))
    return diff_list


def docker_sync_worker():
    while True:
        docker_client = docker.APIClient(base_url='unix://var/run/docker.sock')
        try:
            image_entry = good_image_queue.get(timeout=5)
        except Queue.Empty:
            print 'docker sync queue is empty'
            break
        print 'current queue size is:', good_image_queue.qsize()
        old_tag = (source_registry + '/' + image_entry.get('name') + ':' + image_entry.get('tag'))
        print old_tag
        new_tag = (destination_registry + '/' + image_entry.get('name') + ':' + image_entry.get('tag'))
        print threading.current_thread(), 'pulling image', old_tag
        try:
            print 'pulling image:', old_tag
            docker_client.pull(old_tag)
            print 'tagging image {} to {}:', old_tag, new_tag
            docker_client.tag(old_tag, new_tag)
            print 'pushing image:', new_tag
            docker_client.push(new_tag)
            print 'deleting images', old_tag, new_tag
            docker_client.remove_image(old_tag)
            docker_client.remove_image(new_tag)
        except Exception as e:
            print ('Error: {}', e)
            continue
        good_image_queue.task_done()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--to', help='Destination registry without https',
                        action='store', required=True, dest='destination_registry')
    parser.add_argument('--from', help='Source registry without https',
                        action='store', required=True, dest='source_registry')
    parser.add_argument('--dry-run', help='Make calculations without actual sync',
                        action='store_true', dest='dry_run')
    parser.add_argument('--threads-num', help='threads to run',
                        action='store', type=int, default=1, dest='concurrency')
    parser.add_argument('--print-list', help='print good images to be synced',
                        action='store_true', dest='print_list')
    args = parser.parse_args()
    global destination_registry
    global source_registry
    global concurrency
    destination_registry = args.destination_registry
    source_registry = args.source_registry
    concurrency = args.concurrency
    print 'creating repo list for: ', source_registry
    source_registry_list = get_docker_registry_list(source_registry)
    print 'creating repo list for: ', destination_registry
    destination_registry_list = get_docker_registry_list(destination_registry)
    print 'creating repo diff list...'
    difftags_list = get_diff_list(source_registry_list, destination_registry_list)
    print 'validating manifests for repo diff list...'
    difftags_list_size = len(difftags_list)
    validate_time = time.time()
    for image_entry in difftags_list:
        validate_queue.put(image_entry, timeout=2)

    validate_threads = []
    for i in range(concurrency):
        t = threading.Thread(target=validate_registry_list)
        t.start()
        validate_threads.append(t)

    for t in validate_threads:
        t.join()

    print 'total number of images in diff list:', difftags_list_size
    print 'manifest list validate time is: ', (time.time() - validate_time)
    print '\n number of good images to sync: ', good_image_queue.qsize()

    if args.print_list:
        print 'good images to sync:'
        while True:
            try:
                good_tag = good_image_queue.get(block=False)
            except Queue.Empty:
                break
            print good_tag.get('name') + ":" + good_tag.get('tag')
            good_image_queue.task_done()

    if args.dry_run is False:
        print 'manifest list validate time is: ', (time.time() - validate_time)
        print '\n good images to sync: ', good_image_queue.qsize()
        print 'syncing images'

        sync_threads = []
        for i in range(args.concurrency):
            t = threading.Thread(target=docker_sync_worker)
            t.start()
            validate_threads.append(t)

        for t in sync_threads:
            t.join()


if __name__ == '__main__':
    main()
