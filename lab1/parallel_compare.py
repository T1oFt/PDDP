import threading
import multiprocessing
import time

def get_diff(a: list, b: list, l: int, r: int) -> int:
    return sum(a[i] != b[i] for i in range(l, r))

def run_sequential(a, b):
    start = time.perf_counter()
    res = get_diff(a, b, 0, len(a))
    end = time.perf_counter()
    return res, end - start

def run_threaded(a, b, num_threads=4):
    n = len(a)
    chunk_size = max(1, n // num_threads)
    results = [0] * num_threads
    threads = []

    def worker(tid, l, r):
        results[tid] = get_diff(a, b, l, r)

    start = time.perf_counter()
    
    for i in range(num_threads):
        l = i * chunk_size
        r = min(l + chunk_size, n) if i < num_threads - 1 else n
        if l >= n: break
        t = threading.Thread(target=worker, args=(i, l, r))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()
        
    end = time.perf_counter()
    return sum(results), end - start

def run_multiprocessed(a, b, num_processes=4):
    n = len(a)
    chunk_size = max(1, n // num_processes)
    tasks = []
    
    for i in range(num_processes):
        l = i * chunk_size
        r = min(l + chunk_size, n) if i < num_processes - 1 else n
        if l >= n: break
        tasks.append((a, b, l, r))

    start = time.perf_counter()
    with multiprocessing.Pool(processes=num_processes) as pool:
        results = pool.map(worker_mp, tasks)
    end = time.perf_counter()
    
    return sum(results), end - start

def worker_mp(args):
    a, b, l, r = args
    return get_diff(a, b, l, r)

def print_stats(name, result, time_taken, base_time, workers):
    if time_taken == 0: return
    speedup = base_time / time_taken
    efficiency = (speedup / workers) * 100
    print(f"{name:12}: Рез={result:,}, Время={time_taken:.4f}с, "
          f"Ускорение={speedup:.2f}x, Эффективность={efficiency:.1f}%")

if __name__ == '__main__':
    size = 5_000_000
    a = list(range(size))
    b = [x + (1 if x % 2 == 0 else 0) for x in range(size)]
    workers_count = 8

    print(f"Данные: {size}, Воркеры: {workers_count}\n")

    res_seq, time_seq = run_sequential(a, b)
    print_stats("Sequential", res_seq, time_seq, time_seq, 1)

    res_thr, time_thr = run_threaded(a, b, num_threads=workers_count)
    print_stats("Threading", res_thr, time_thr, time_seq, workers_count)

    res_mp, time_mp = run_multiprocessed(a, b, num_processes=workers_count)
    print_stats("Multiproc", res_mp, time_mp, time_seq, workers_count)
