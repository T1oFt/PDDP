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

def worker_mp(args):
    a, b, l, r = args
    return get_diff(a, b, l, r)

if __name__ == '__main__':
    size = 5_000_000_000
    a = list(range(size))
    b = [x + (1 if x % 2 == 0 else 0) for x in range(size)]
    workers_count = 8

    print(f"Размер данных: {size}, Кол-во воркеров: {workers_count}\n")

    res_seq, time_seq = run_sequential(a, b)
    print(f"Sequential:  Результат={res_seq}, Время={time_seq:.4f} сек")

    res_thr, time_thr = run_threaded(a, b, num_threads=workers_count)
    print(f"Threading:   Результат={res_thr}, Время={time_thr:.4f} сек (ускорение: {time_seq/time_thr:.2f}x)")
