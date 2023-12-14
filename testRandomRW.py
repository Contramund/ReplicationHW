#!/usr/bin/env python3

import subprocess
import time
import random
import requests
import sys
import os

counter = 0

ports = {
    "server-0": "127.0.0.1:8080",
    "server-1": "127.0.0.1:8081",
    "server-2": "127.0.0.1:8082",
    "server-3": "127.0.0.1:8083",
    "server-4": "127.0.0.1:8084"
}

topology = {
    "server-0": ["server-1", "server-2", "server-3", "server-4"],
    "server-1": ["server-0", "server-2", "server-3", "server-4"],
    "server-2": ["server-1", "server-0", "server-3", "server-4"],
    "server-3": ["server-1", "server-2", "server-0", "server-4"],
    "server-4": ["server-1", "server-2", "server-3", "server-0"]
}


def testRound():
    global counter

    originNum = random.randrange(len(ports))
    originName = list(ports)[originNum]
    originAddr = ports[originName]
    print(f"Write to \"{originName}\"")

    reqVersion = "version-" + str(counter)
    counter += 1
    reqBody = {
        "op": "add",
        "path": "/" + originName,
        "value": reqVersion
    }
    res = requests.post("http://" + originAddr + "/replace", json=[reqBody])

    # it's ok to ask same server (it may store our patch in cache)
    sinkNum = random.randrange(len(ports))
    sinkName = list(ports)[sinkNum]
    sinkAddr = ports[sinkName]
    print(f"Read back from \"{sinkName}\"")

    now = time.time()
    while True:
        if time.time() - now > 5:
            print("Overtime\n")
            break
        res = requests.get("http://" + sinkAddr + "/get").json()
        if originName in res:
            if res[originName] == reqVersion:
                print(f'Spent {time.time() - now} seconds\n')
                break


def testThemAll():
    print("Testing:")
    for _ in range(20):
        testRound()
    input("Continue?")


if __name__ == "__main__":
    servers = {}

    print('Starting up servers:')
    for name, port in ports.items():
        print(f'  Starting up \"{name}\"... ', end="")
        proc = subprocess.Popen(
            [
                os.path.join(os.path.abspath(sys.path[0]), "./replicaStorage"),
                "-n",
                name,
                "-p",
                port
            ] + [ports[i] for i in topology[name]],
            stdout=subprocess.DEVNULL)
        servers[name] = proc
        print("Done.")

    # just to init properly (maybe delete)
    time.sleep(1)

    testThemAll()

    print('Killing up servers:')
    for name, proc in servers.items():
        print(f'  Sending SIGTERM to \"{name}\"... ', end="")
        proc.terminate()
        print("Done.")

    for _ in range(9):
        for name, proc in servers.items():
            try:
                proc.wait(1)
            except subprocess.TimeoutExpired:
                None

    for name, proc in servers.items():
        try:
            proc.wait(1)
        except subprocess.TimeoutExpired:
            print(f'  Server \"{name}\" don\'t wanna die. Killing it... ', end="")
            proc.terminate()
            print("Done.")

    print("Testing finished.")

