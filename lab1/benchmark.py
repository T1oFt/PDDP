import threading
import multiprocessing
import time
import matplotlib.pyplot as plt
from typing import List, Tuple, Dict


def get_diff(a: list, b: list, l: int, r: int) -> int:
    return sum(1 for i in range(l, r) if a[i] != b[i])

def run_sequential(a: list, b: list) -> int:
    return get_diff(a, b, 0, len(a))

def run_threaded(a: list, b: list, num_threads: int) -> int:
    n = len(a)
    chunk_size = max(1, n // num_threads)
    results = [0] * num_threads
    threads = []

    def worker(tid: int, l: int, r: int):
        results[tid] = get_diff(a, b, l, r)

    for i in range(num_threads):
        l = i * chunk_size
        r = n if i == num_threads - 1 else min(l + chunk_size, n)
        
        if l >= n: break
        
        t = threading.Thread(target=worker, args=(i, l, r))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    return sum(results)

def generate_data(size: int) -> Tuple[List[int], List[int]]:
    a = list(range(size))
    b = [x + 1 if x % 2 == 0 else x for x in range(size)]
    return a, b

def calculate_metrics(t_seq: float, t_par: float, workers: int) -> Dict[str, float]:
    speedup = t_seq / t_par if t_par > 0 else 0
    efficiency = (speedup / workers) * 100 if workers > 0 else 0
    return {
        "time": t_par,
        "speedup": speedup,
        "efficiency": efficiency
    }

def test_determinism(iterations: int = 105):
    print(f"\n--- Тест детерминизма ({iterations} запусков) ---")
    size = 100_000
    a, b = generate_data(size)
    
    expected_result = run_sequential(a, b)
    workers = 4
    
    results = []
    for _ in range(iterations):
        res = run_threaded(a, b, workers)
        results.append(res)
    
    unique_results = set(results)
    
    if len(unique_results) == 1 and list(unique_results)[0] == expected_result:
        print(f"[OK] Детерминизм подтвержден. Все {iterations} запусков вернули результат: {expected_result}")
        return True
    else:
        print(f"[FAIL] Обнаружена недетерминированность!")
        print(f"Ожидалось: {expected_result}")
        print(f"Получены уникальные значения: {unique_results}")
        return False

def run_benchmark_experiments():
    print("\n--- Запуск экспериментов по масштабированию ---")
    
    sizes = [1_000_000, 5_000_000]
    max_cpu = multiprocessing.cpu_count()

    thread_configs = set()
    thread_configs.add(1)
    thread_configs.add(2)
    
    for i in range(1, max_cpu + 1):
        thread_configs.add(i)
        
    thread_configs.add(max_cpu * 2)
    thread_configs.add(max_cpu * 4)
    
    max_workers_limit = max_cpu * 4
    workers_list = sorted([w for w in thread_configs if 1 <= w <= max_workers_limit])
    
    print(f"Доступно ядер CPU: {max_cpu}")
    print(f"Тестируемое кол-во потоков: {workers_list}")
    
    all_stats = {}

    for size in sizes:
        print(f"\nГенерация данных размера {size:,}...")
        a, b = generate_data(size)

        seq_times = []
        seq_res = None
        for _ in range(3):
            start = time.perf_counter()
            seq_res = run_sequential(a, b)
            end = time.perf_counter()
            seq_times.append(end - start)
        t_seq = sum(seq_times) / len(seq_times)
        
        print(f"Sequential время (среднее): {t_seq:.4f} сек, Результат: {seq_res}")
        
        all_stats[size] = {}
        
        for workers in workers_list:
            par_times = []
            par_res = None
            
            repeats = 3 if workers <= max_cpu * 2 else 1
            
            for _ in range(repeats):
                start = time.perf_counter()
                par_res = run_threaded(a, b, workers)
                end = time.perf_counter()
                par_times.append(end - start)
            
            t_par = sum(par_times) / len(par_times)
            
            if par_res != seq_res:
                print(f"[WARN] Несовпадение результатов при {workers} потоках! Ож: {seq_res}, Пол: {par_res}")
            
            metrics = calculate_metrics(t_seq, t_par, workers)
            all_stats[size][workers] = metrics
            
            print(f"Workers: {workers:3d} | Time: {t_par:.4f}s | Speedup: {metrics['speedup']:.2f}x | Eff: {metrics['efficiency']:.1f}%")

    return all_stats, max_cpu

def plot_results(all_stats: Dict, max_cpu: int):
    colors = ['blue', 'green', 'red', 'orange']
    
    fig, axs = plt.subplots(1, 3, figsize=(18, 6))
    fig.suptitle('Метрики производительности GIL', fontsize=16)
    
    metrics_names = ['time', 'speedup', 'efficiency']
    titles = ['Время выполнения (сек)', 'Ускорение (Speedup)', 'Эффективность (%)']
    ylabels = ['Время (с)', 'Коэф. ускорения', 'Эффективность (%)']

    first_size = next(iter(all_stats))
    workers_list = sorted(list(all_stats[first_size].keys()))
    
    for idx, metric in enumerate(metrics_names):
        ax = axs[idx]
        ax.set_xlabel('Количество потоков')
        ax.set_ylabel(ylabels[idx])
        ax.set_title(titles[idx])
        ax.grid(True, linestyle='--', alpha=0.6)

        ax.axvline(x=8, color='gray', linestyle=':', label=f'CPU Cores ({8})')
        ax.axvline(x=max_cpu, color='gray', linestyle=':', label=f'CPU Cores ({max_cpu})')
        if max_cpu * 2 in workers_list:
             ax.axvline(x=max_cpu*2, color='gray', linestyle=':', alpha=0.5)

        for i, (size, stats) in enumerate(all_stats.items()):
            ws = sorted(stats.keys())
            vals = [stats[w][metric] for w in ws]
            label = f"Size: {size:,}"
            ax.plot(ws, vals, marker='o', label=label, color=colors[i % len(colors)])
            
            if metric == 'efficiency':
                ax.axhline(y=100, color='green', linestyle='-', alpha=0.2, label='Ideal Eff.')

        ax.legend()
    
    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    plt.savefig("performance_metrics_GIL.png")
    print("\nГрафики сохранены в файл: performance_metrics.png")
    plt.show()

if __name__ == '__main__':
    is_deterministic = test_determinism(iterations=105)
    

    stats, cpu_count = run_benchmark_experiments()
    
    plot_results(stats, cpu_count)