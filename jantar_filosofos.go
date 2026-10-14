package main

import (
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"
)

func runJantar(numeroDeFilosofos, refeicoes int, modo string) {
	garfos := make([]chan struct{}, numeroDeFilosofos)
	for indice := range garfos {
		garfos[indice] = make(chan struct{}, 1)
		garfos[indice] <- struct{}{}
	}

	var grupoDeEspera sync.WaitGroup
	inicio := time.Now()
	esperaTotalPorFilosofo := make([]time.Duration, numeroDeFilosofos)
	refeicoesPorFilosofo := make([]int, numeroDeFilosofos)

	if modo == "deadlock" {
		inicioSimultaneo := make(chan struct{})
		garfosPegosEsquerda := make(chan struct{}, numeroDeFilosofos)
		todosProntosNaEsquerda := make(chan struct{})

		go func() {
			for i := 0; i < numeroDeFilosofos; i++ {
				<-garfosPegosEsquerda
			}
			close(todosProntosNaEsquerda)
		}()

		for indice := 0; indice < numeroDeFilosofos; indice++ {
			grupoDeEspera.Add(1)
			go func(idFilosofo int) {
				defer grupoDeEspera.Done()
				<-inicioSimultaneo
				<-garfos[idFilosofo]
				garfosPegosEsquerda <- struct{}{}
				<-todosProntosNaEsquerda
				<-garfos[(idFilosofo+1)%numeroDeFilosofos]

				time.Sleep(20 * time.Millisecond)
				garfos[idFilosofo] <- struct{}{}
				garfos[(idFilosofo+1)%numeroDeFilosofos] <- struct{}{}

				fmt.Printf("Filosofo %d terminou refeicao 1 em %s\n", idFilosofo, time.Since(inicio).Round(time.Millisecond))
			}(indice)
		}

		close(inicioSimultaneo)
		concluido := make(chan struct{})
		go func() {
			grupoDeEspera.Wait()
			close(concluido)
		}()

		select {
		case <-concluido:
			fmt.Println("Todas as refeicoes terminaram.")
		case <-time.After(2 * time.Second):
			fmt.Println("Deadlock detectado: todos os filosofos seguram o garfo da esquerda e esperam o da direita.")
		}
		return
	}

	lugaresNaMesa := make(chan struct{}, numeroDeFilosofos-1)

	for indice := 0; indice < numeroDeFilosofos; indice++ {
		grupoDeEspera.Add(1)
		go func(idFilosofo int) {
			defer grupoDeEspera.Done()

			for numeroDaRefeicao := 0; numeroDaRefeicao < refeicoes; numeroDaRefeicao++ {
				inicioDaEspera := time.Now()
				if modo == "limite" {
					lugaresNaMesa <- struct{}{}
				}

				garfoDaEsquerda, garfoDaDireita := idFilosofo, (idFilosofo+1)%numeroDeFilosofos
				if modo == "safe" && idFilosofo == numeroDeFilosofos-1 {
					garfoDaEsquerda, garfoDaDireita = (idFilosofo+1)%numeroDeFilosofos, idFilosofo
				}

				<-garfos[garfoDaEsquerda]
				<-garfos[garfoDaDireita]
				esperaTotalPorFilosofo[idFilosofo] += time.Since(inicioDaEspera)

				time.Sleep(20 * time.Millisecond)
				garfos[garfoDaEsquerda] <- struct{}{}
				garfos[garfoDaDireita] <- struct{}{}
				refeicoesPorFilosofo[idFilosofo]++
				if modo == "limite" {
					<-lugaresNaMesa
				}
			}
		}(indice)
	}

	concluido := make(chan struct{})
	go func() {
		grupoDeEspera.Wait()
		close(concluido)
	}()

	tempoLimite := time.Duration(refeicoes)*100*time.Millisecond + time.Second
	select {
	case <-concluido:
		fmt.Println("Todas as refeicoes terminaram.")
		menorEspera := time.Duration(1<<63 - 1)
		var maiorEspera time.Duration
		var somaDasEsperas time.Duration
		for indice := 0; indice < numeroDeFilosofos; indice++ {
			esperaMedia := esperaTotalPorFilosofo[indice] / time.Duration(refeicoesPorFilosofo[indice])
			somaDasEsperas += esperaTotalPorFilosofo[indice]
			if esperaMedia < menorEspera {
				menorEspera = esperaMedia
			}
			if esperaMedia > maiorEspera {
				maiorEspera = esperaMedia
			}
			fmt.Printf("Filosofo %d: refeicoes=%d, espera media pelos garfos=%s\n",
				indice, refeicoesPorFilosofo[indice], esperaMedia.Round(time.Microsecond))
		}
		esperaMediaGeral := somaDasEsperas / time.Duration(numeroDeFilosofos*refeicoes)
		fmt.Printf("Resumo: estrategia=%s, filosofos=%d, refeicoes_por_filosofo=%d, tempo_total=%s, espera_media_geral=%s, menor_maior_espera=%s/%s\n",
			modo, numeroDeFilosofos, refeicoes, time.Since(inicio).Round(time.Millisecond),
			esperaMediaGeral.Round(time.Microsecond), menorEspera.Round(time.Microsecond), maiorEspera.Round(time.Microsecond))
	case <-time.After(tempoLimite):
		fmt.Println("Tempo limite excedido sem terminar.")
	}
}

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Uso: go run jantar_filosofos.go <N> <refeicoes> [deadlock|safe|limite]")
		return
	}

	numeroDeFilosofos, erro := strconv.Atoi(os.Args[1])
	if erro != nil || numeroDeFilosofos < 2 {
		fmt.Println("N precisa ser um numero inteiro maior ou igual a 2.")
		return
	}

	refeicoes, erro := strconv.Atoi(os.Args[2])
	if erro != nil || refeicoes < 1 {
		fmt.Println("refeicoes precisa ser um numero inteiro maior que 0.")
		return
	}

	modo := "safe"
	if len(os.Args) > 3 {
		modo = os.Args[3]
	}

	if modo == "deadlock" {
		fmt.Println("Exemplo de deadlock: todos pegam primeiro o garfo da esquerda.")
		runJantar(numeroDeFilosofos, refeicoes, modo)
		return
	}

	if modo == "limite" {
		fmt.Println("Versao segura: no maximo N-1 filosofos tentam pegar garfos ao mesmo tempo.")
	} else {
		modo = "safe"
		fmt.Println("Versao segura: quebra de simetria com hierarquia de garfos.")
	}
	runJantar(numeroDeFilosofos, refeicoes, modo)
}
