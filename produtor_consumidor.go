package main

import (
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type BufferCanal struct {
	buffer chan int
}

func novoBufferCanal(capacidade int) *BufferCanal {
	return &BufferCanal{buffer: make(chan int, capacidade)}
}

func executarCanal(produtores, consumidores, total, capacidade int) {
	buffer := novoBufferCanal(capacidade)
	var grupoProdutores sync.WaitGroup
	var grupoConsumidores sync.WaitGroup
	var itensProduzidos int64
	var itensConsumidos int64
	itensPorConsumidor := make([]int64, consumidores)
	var somaOcupacao int64
	var numeroDeAmostras int64
	inicio := time.Now()
	pararAmostragem := make(chan struct{})
	var grupoAmostrador sync.WaitGroup

	grupoAmostrador.Add(1)
	go func() {
		defer grupoAmostrador.Done()
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				atomic.AddInt64(&somaOcupacao, int64(len(buffer.buffer)))
				atomic.AddInt64(&numeroDeAmostras, 1)
			case <-pararAmostragem:
				return
			}
		}
	}()

	for indice := 0; indice < produtores; indice++ {
		grupoProdutores.Add(1)
		go func(idProdutor int) {
			defer grupoProdutores.Done()
			for sequencia := idProdutor; sequencia < total; sequencia += produtores {
				valor := sequencia + 1
				buffer.buffer <- valor
				atomic.AddInt64(&itensProduzidos, 1)
				time.Sleep(10 * time.Millisecond)
			}
		}(indice)
	}

	for indice := 0; indice < consumidores; indice++ {
		grupoConsumidores.Add(1)
		go func(idConsumidor int) {
			defer grupoConsumidores.Done()
			for {
				select {
				case _, ok := <-buffer.buffer:
					if !ok {
						return
					}
					atomic.AddInt64(&itensConsumidos, 1)
					atomic.AddInt64(&itensPorConsumidor[idConsumidor], 1)
				case <-time.After(200 * time.Millisecond):
					if atomic.LoadInt64(&itensProduzidos) >= int64(total) && atomic.LoadInt64(&itensConsumidos) >= int64(total) {
						return
					}
				}
			}
		}(indice)
	}

	grupoProdutores.Wait()
	close(buffer.buffer)
	grupoConsumidores.Wait()
	close(pararAmostragem)
	grupoAmostrador.Wait()
	duracao := time.Since(inicio)

	imprimirResultados("channel", total, capacidade, duracao,
		atomic.LoadInt64(&itensProduzidos), atomic.LoadInt64(&itensConsumidos),
		itensPorConsumidor, atomic.LoadInt64(&somaOcupacao), atomic.LoadInt64(&numeroDeAmostras))
}

type SemBuffer struct {
	itens    []int
	in       int
	out      int
	ocupacao int
	mu       sync.Mutex
	nfull    chan struct{}
	nempt    chan struct{}
}

func novoSemBuffer(capacidade int) *SemBuffer {
	b := &SemBuffer{
		itens: make([]int, capacidade),
		nfull: make(chan struct{}, capacidade),
		nempt: make(chan struct{}, capacidade),
	}
	for i := 0; i < capacidade; i++ {
		b.nfull <- struct{}{}
	}
	return b
}

func (b *SemBuffer) colocar(valor int) {
	<-b.nfull
	b.mu.Lock()
	b.itens[b.in] = valor
	b.in = (b.in + 1) % len(b.itens)
	b.ocupacao++
	b.mu.Unlock()
	b.nempt <- struct{}{}
}

func executarSemaforo(produtores, consumidores, total, capacidade int) {
	buffer := novoSemBuffer(capacidade)
	var grupoProdutores sync.WaitGroup
	var grupoConsumidores sync.WaitGroup
	var itensProduzidos int64
	var itensConsumidos int64
	itensPorConsumidor := make([]int64, consumidores)
	var somaOcupacao int64
	var numeroDeAmostras int64
	inicio := time.Now()
	pararAmostragem := make(chan struct{})
	var grupoAmostrador sync.WaitGroup

	grupoAmostrador.Add(1)
	go func() {
		defer grupoAmostrador.Done()
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				buffer.mu.Lock()
				ocupacaoAtual := buffer.ocupacao
				buffer.mu.Unlock()
				atomic.AddInt64(&somaOcupacao, int64(ocupacaoAtual))
				atomic.AddInt64(&numeroDeAmostras, 1)
			case <-pararAmostragem:
				return
			}
		}
	}()

	for indice := 0; indice < produtores; indice++ {
		grupoProdutores.Add(1)
		go func(idProdutor int) {
			defer grupoProdutores.Done()
			for sequencia := idProdutor; sequencia < total; sequencia += produtores {
				valor := sequencia + 1
				buffer.colocar(valor)
				atomic.AddInt64(&itensProduzidos, 1)
				time.Sleep(10 * time.Millisecond)
			}
		}(indice)
	}

	for indice := 0; indice < consumidores; indice++ {
		grupoConsumidores.Add(1)
		go func(idConsumidor int) {
			defer grupoConsumidores.Done()
			for {
				select {
				case <-buffer.nempt:
					buffer.mu.Lock()
					buffer.out = (buffer.out + 1) % len(buffer.itens)
					buffer.ocupacao--
					buffer.mu.Unlock()
					buffer.nfull <- struct{}{}
					atomic.AddInt64(&itensConsumidos, 1)
					atomic.AddInt64(&itensPorConsumidor[idConsumidor], 1)
				case <-time.After(150 * time.Millisecond):
					if atomic.LoadInt64(&itensProduzidos) >= int64(total) && atomic.LoadInt64(&itensConsumidos) >= int64(total) {
						return
					}
				}
			}
		}(indice)
	}

	grupoProdutores.Wait()
	for {
		if atomic.LoadInt64(&itensConsumidos) >= int64(total) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	grupoConsumidores.Wait()
	close(pararAmostragem)
	grupoAmostrador.Wait()
	duracao := time.Since(inicio)

	imprimirResultados("semaforo", total, capacidade, duracao,
		atomic.LoadInt64(&itensProduzidos), atomic.LoadInt64(&itensConsumidos),
		itensPorConsumidor, atomic.LoadInt64(&somaOcupacao), atomic.LoadInt64(&numeroDeAmostras))
}

func imprimirResultados(versao string, total, capacidade int, duracao time.Duration,
	produzidos, consumidos int64, porConsumidor []int64, somaOcupacao, amostras int64) {
	ocupacaoMedia := 0.0
	if amostras > 0 {
		ocupacaoMedia = float64(somaOcupacao) / float64(amostras)
	}
	throughput := float64(consumidos) / duracao.Seconds()
	fmt.Printf("Versao=%s, K=%d, produzidos=%d, consumidos=%d, tempo=%s, throughput=%.2f itens/s, ocupacao media=%.6f/%d\n",
		versao, capacidade, produzidos, consumidos, duracao.Round(time.Millisecond), throughput, ocupacaoMedia, capacidade)
	for indice := range porConsumidor {
		fmt.Printf("Consumidor %d: %d itens\n", indice, atomic.LoadInt64(&porConsumidor[indice]))
	}
}

func main() {
	if len(os.Args) < 5 {
		fmt.Println("Uso: go run produtor_consumidor.go <P> <C> <total> <capacidade> [canal|semaforo]")
		return
	}

	produtores, erroProdutores := strconv.Atoi(os.Args[1])
	consumidores, erroConsumidores := strconv.Atoi(os.Args[2])
	total, erroTotal := strconv.Atoi(os.Args[3])
	capacidade, erroCapacidade := strconv.Atoi(os.Args[4])
	if erroProdutores != nil || erroConsumidores != nil || erroTotal != nil || erroCapacidade != nil {
		fmt.Println("Todos os parametros devem ser inteiros.")
		return
	}
	if produtores < 1 || consumidores < 1 || total < 1 || capacidade < 1 {
		fmt.Println("P, C, total e capacidade devem ser maiores que zero.")
		return
	}

	modo := "canal"
	if len(os.Args) > 5 {
		modo = os.Args[5]
	}

	if modo == "semaforo" {
		fmt.Println("Execucao com semaforos.")
		executarSemaforo(produtores, consumidores, total, capacidade)
		return
	}

	fmt.Println("Execucao com channel.")
	executarCanal(produtores, consumidores, total, capacidade)
}
