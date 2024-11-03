package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/joho/godotenv"
)

const delay time.Duration = 50 * time.Millisecond

// WorkerPool представляет собой структуру пула воркеров.
type WorkerPool struct {
	jobs         chan string     // Канал для передачи заданий воркерам
	workers      map[int]*Worker // Мапа воркеров по их ID
	availableIDs []int           // Список доступных ID для переиспользования
	nextID       int             // Следующий уникальный ID, если нет доступных
	wg           sync.WaitGroup  // Для ожидания завершения всех воркеров
	output       *os.File        // Файл для записи результатов
	logger       *log.Logger     // Логгер для вывода информации
}

// Worker представляет собой отдельный воркер.
type Worker struct {
	id   int           // ID воркера
	jobs chan string   // Канал для получения заданий
	quit chan struct{} // Канал для остановки воркера
}

// NewWorker создает нового воркера.
func NewWorker(id int, jobs chan string) *Worker {
	return &Worker{
		id:   id,
		jobs: jobs,
		quit: make(chan struct{}),
	}
}

var translitMap = map[rune]string{
	'а': "a", 'б': "b", 'в': "v", 'г': "g", 'д': "d", 'е': "e", 'ё': "yo", 'ж': "zh", 'з': "z",
	'и': "i", 'й': "y", 'к': "k", 'л': "l", 'м': "m", 'н': "n", 'о': "o", 'п': "p", 'р': "r",
	'с': "s", 'т': "t", 'у': "u", 'ф': "f", 'х': "kh", 'ц': "ts", 'ч': "ch", 'ш': "sh", 'щ': "shch",
	'ъ': "", 'ы': "y", 'ь': "'", 'э': "e", 'ю': "yu", 'я': "ya",
	'А': "A", 'Б': "B", 'В': "V", 'Г': "G", 'Д': "D", 'Е': "E", 'Ё': "Yo", 'Ж': "Zh", 'З': "Z",
	'И': "I", 'Й': "Y", 'К': "K", 'Л': "L", 'М': "M", 'Н': "N", 'О': "O", 'П': "P", 'Р': "R",
	'С': "S", 'Т': "T", 'У': "U", 'Ф': "F", 'Х': "Kh", 'Ц': "Ts", 'Ч': "Ch", 'Ш': "Sh", 'Щ': "Shch",
	'Ъ': "", 'Ы': "Y", 'Ь': "'", 'Э': "E", 'Ю': "Yu", 'Я': "Ya",
}

// some_func имитирует работу с задержкой
func some_func(input string) string {

	time.Sleep(delay)

	//Просто сделал такую функцию для имитации работы :)
	//Переводит кириллицу в транслит. "привет" -> "privet", "Виктор" -> "Viktor", "Хлеб" -> "Khleb"

	output := ""
	for _, char := range input {
		if translitChar, found := translitMap[char]; found {
			output += translitChar
		} else {
			output += string(char)
		}
	}
	return output
}

// Start запускает воркера, который слушает канал заданий
func (w *Worker) Start(wg *sync.WaitGroup, logger *log.Logger, output *os.File) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case job, open := <-w.jobs:
				if !open {
					logger.Printf("Воркер %d: канал задач закрыт! Завершил работу", w.id)
					return
				}
				processedJob := some_func(job)
				fmt.Fprintln(output, processedJob)
				logger.Printf("Воркер %d: обработал строку: %s", w.id, job)

			case <-w.quit:
				logger.Printf("Воркер %d: получен сигнал остановки! Завершил работу", w.id)
				return
			}
		}
	}()
}

// Stop останавливает воркер
func (w *Worker) Stop() {
	close(w.quit)
}

// NewWorkerPool создает новый пул воркеров
func NewWorkerPool(logger *log.Logger, output *os.File) *WorkerPool {
	return &WorkerPool{
		jobs:    make(chan string),
		workers: make(map[int]*Worker),
		logger:  logger,
		output:  output,
		nextID:  1,
	}
}

// AddWorker добавляет новый воркер в пул
func (wp *WorkerPool) AddWorker() {
	ID := wp.nextID
	if len(wp.availableIDs) > 0 {
		ID = wp.availableIDs[0]
		wp.availableIDs = wp.availableIDs[1:]
	} else {
		wp.nextID++
	}

	worker := NewWorker(ID, wp.jobs)
	worker.Start(&wp.wg, wp.logger, wp.output)
	wp.workers[ID] = worker
	wp.logger.Printf("Вооркер %d запущен\n", worker.id)
	fmt.Printf("Воркер %d запущен\n", worker.id)
}

// RemoveWorker удаляет воркер из пула
func (wp *WorkerPool) RemoveWorker(id int) {
	worker, exists := wp.workers[id]
	if !exists {
		wp.logger.Printf("Воркер с ID %d не найден\n", id)
		fmt.Printf("Воркер с ID %d не найден\n", worker.id)
		return
	}

	worker.Stop()
	delete(wp.workers, id)
	wp.availableIDs = append(wp.availableIDs, id)
	fmt.Fprintf(wp.output, "Воркер %d остановлен\n", worker.id)
	fmt.Printf("Воркер %d остановлен\n", worker.id)
}

// Shutdown завершает работу пула, дожидаясь окончания всех воркеров
func (wp *WorkerPool) Shutdown() {
	for id := range wp.workers {
		wp.RemoveWorker(id)
	}
	wp.wg.Wait() // Ожидаем завершения всех воркеров
	wp.logger.Printf("Worker pool завершил работу!")
	fmt.Println("Worker pool завершил работу!")
}


// ProcessFile читает строки из файла и отправляет их в канал jobs.
func (wp *WorkerPool) ProcessFile(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		wp.jobs <- scanner.Text() // Отправляем строку в канал
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	// Закрываем канал jobs после окончания чтения файла
	close(wp.jobs)
	return nil
}

func main() {
	err := godotenv.Load("cfg.env")
	if err != nil {
		log.Fatalf("Error loading cfg.env file")
	}

	inputFilePath := os.Getenv("INPUT_FILE")
	outputFilePath := os.Getenv("OUTPUT_FILE")
	logFilePath := os.Getenv("LOG_FILE")

	outputFile, err := os.Create(outputFilePath)
	if err != nil {
		log.Fatalf("Error creating output file: %v", err)
	}
	defer outputFile.Close()

	logFile, err := os.Create(logFilePath)
	if err != nil {
		log.Fatalf("Error creating log file: %v", err)
	}
	defer logFile.Close()

	logger := log.New(logFile, "", log.LstdFlags)

	wp := NewWorkerPool(logger, outputFile)


	file_end := make(chan struct{})
	done := make(chan struct{})
	
	go func() {
		fmt.Println("Выберете команду:")
		fmt.Println("1 - запустить воркер")
		fmt.Println("2 - запустить несколько воркеров")
		fmt.Println("3 - остановить воркер по ID")
		fmt.Println("4 - Список запущенных воркеров")
		fmt.Println("0 - выйти из программы")
		for {
			select {
			case <-file_end:
				return
			default:
				var menu_num int
				if _, err := fmt.Scan(&menu_num); err != nil {
				    return
				}
				switch menu_num {
				case 1:
					wp.AddWorker()
				case 2:
					fmt.Print("Введите кол-во воркеров для запуска: ")
					var count int
					if _, err := fmt.Scan(&count); err != nil {
					    return
					}
					for range(count){
						wp.AddWorker()
					}
				case 3:
					if len(wp.workers) == 0 {
						fmt.Println("Нет запущенных воркеров для остановки!")
						continue
					}
					fmt.Print("Введите ID воркера для остановки: ")
					var id int
					if _, err := fmt.Scan(&id); err != nil {
					    return
					}
					wp.RemoveWorker(id)

				case 4:
					if len(wp.workers) != 0 {
						fmt.Print("Список активных воркеров:")
						for id, _ := range wp.workers { // Проходим по каждому ID воркера
							fmt.Printf(" %d", id) // Выводим ID воркера
						}
						fmt.Printf("\n")
					} else {
						fmt.Println("Нет запущенных воркеров")
					}
				case 0:
					fmt.Println("Завершение работы!")
					wp.Shutdown()
					close(done)
					return
				default:
					fmt.Println("Неизвестная команда")
				}
			}
		}
	}()

	// Чтение данных из файла и отправка их в канал
	go func() {
		if err := wp.ProcessFile(inputFilePath); err != nil {
			fmt.Fprintln(outputFile, "Error reading file:", err)
		}
		close(file_end) // Сигнализируем о завершении работы с файлом

		fmt.Println("Чтение завершено. Завершение работы!")

		wp.Shutdown()
		close(done)
	}()

	<-done
}
