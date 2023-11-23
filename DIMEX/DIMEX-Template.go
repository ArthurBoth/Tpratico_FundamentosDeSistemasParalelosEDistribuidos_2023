/*  Construido como parte da disciplina: FPPD - PUCRS - Escola Politecnica
    Professor: Fernando Dotti  (https://fldotti.github.io/)
    Modulo representando Algoritmo de Exclusão Mútua Distribuída:
    Semestre 2023/1
	Aspectos a observar:
	   mapeamento de módulo para estrutura
	   inicializacao
	   semantica de concorrência: cada evento é atômico
	   							  módulo trata 1 por vez
	Q U E S T A O
	   Além de obviamente entender a estrutura ...
	   Implementar o núcleo do algoritmo ja descrito, ou seja, o corpo das
	   funcoes reativas a cada entrada possível:
	   			handleUponReqEntry()  // recebe do nivel de cima (app)
				handleUponReqExit()   // recebe do nivel de cima (app)
				handleUponDeliverRespOk(msgOutro)   // recebe do nivel de baixo
				handleUponDeliverReqEntry(msgOutro) // recebe do nivel de baixo
*/

package DIMEX

import (
	PP2PLink "SD/PP2PLink"
	"fmt"
	"strconv"
	"strings"
)

// ------------------------------------------------------------------------------------
// ------- principais tipos
// ------------------------------------------------------------------------------------

const (
	respOk = "respOk"
	reqEntry = "reqEntry"
)

type State int // enumeracao dos estados possiveis de um processo
const (
	outMX State = iota
	wantMX
	inMX
)

type dmxReq int // enumeracao dos estados possiveis de um processo
const (
	ENTER dmxReq = iota
	EXIT
)

type dmxResp struct { // mensagem do módulo DIMEX infrmando que pode acessar - pode ser somente um sinal (vazio)
	// mensagem para aplicacao indicando que pode prosseguir
}

type DIMEX_Module struct {
	Req         chan dmxReq  // canal para receber pedidos da aplicacao (REQ e EXIT)
	Ind         chan dmxResp // canal para informar aplicacao que pode acessar
	addresses   []string     // endereco de todos, na mesma ordem
	id          int          // identificador do processo - é o indice no array de enderecos acima
	estadoAtual State        // estado deste processo na exclusao mutua distribuida
	waiting     []bool       // processos aguardando tem flag true
	relogioLoc  int          // relogio logico local
	lastReqTime int          // timestamp local da ultima requisicao deste processo
	numeroRespostasRecebidas	int		   		// numero de respostas recebidas 
	isDebugging bool // wether to show prints for logs or not

	Pp2plink *PP2PLink.PP2PLink // acesso a comunicacao enviar por PP2PLinq.Req  e receber por PP2PLinq.Ind
}

// ------------------------------------------------------------------------------------
// ------- inicializacao
// ------------------------------------------------------------------------------------

func NewDIMEX(_addresses []string, _id int, _dbg bool) *DIMEX_Module {

	p2p := PP2PLink.NewPP2PLink(_addresses[_id], _dbg)
	dmx := &DIMEX_Module{
		Req: make(chan dmxReq, 1),
		Ind: make(chan dmxResp, 1),

		addresses:   _addresses,
		id:          _id,
		estadoAtual: outMX,
		waiting:     make([]bool, len(_addresses)),
		relogioLoc:  0,
		lastReqTime: 0,
		isDebugging: _dbg,

		Pp2plink: p2p}

	for i := 0; i < len(dmx.waiting); i++ {
		dmx.waiting[i] = false
	}
	dmx.Start()
	dmx.outDbg("Init DIMEX!")
	return dmx
}

// ------------------------------------------------------------------------------------
// ------- nucleo do funcionamento
// ------------------------------------------------------------------------------------

func (module *DIMEX_Module) Start() {
	i := 0
	go func() {
		for {
			fmt.Println("\n\\n\n\tEaí meu chapa: ", i, "\n\\n\n")
			i++
			select {
			case dmxR := <-module.Req: // vindo da  aplicação
				if dmxR == ENTER {
					module.outDbg("app pede mx")
					module.handleUponReqEntry()

				} else if dmxR == EXIT {
					module.outDbg("app libera mx")
					module.handleUponReqExit()
				}

			case msgOutro := <-module.Pp2plink.Ind: // vindo de outro processo
				fmt.Print("dimex recebe da rede: ", msgOutro)
				if strings.Contains(msgOutro.Message, respOk) {
					module.outDbg("         <<<---- responde! " + msgOutro.Message)
					module.handleUponDeliverRespOk(msgOutro)

				} else if strings.Contains(msgOutro.Message, reqEntry) {
					module.outDbg("          <<<---- pede??  " + msgOutro.Message)
					module.handleUponDeliverReqEntry(msgOutro)

				}
			}
		}
	}()
}

// ------------------------------------------------------------------------------------
// ------- tratamento de pedidos vindos da aplicacao
// ------- UPON ENTRY
// ------- UPON EXIT
// ------------------------------------------------------------------------------------

func (module *DIMEX_Module) handleUponReqEntry() {
	/*
		upon event [ dmx, Entry  |  r ]  do
			lts.ts++
			myTs := lts
			resps := 0
			para todo processo p
				trigger [ pl , Send | [ reqEntry, r, myTs ]
			estado := queroSC
	*/
	fmt.Println("===== handleUponReqEntry =====", module.id)
	module.relogioLoc++
	module.lastReqTime = module.relogioLoc
	module.numeroRespostasRecebidas = 0
	var pl string
	var leadingSpaces string
	for i := 0; i < len(module.addresses); i++ {
		if module.id == i {
			continue
		}
		pl = module.addresses[i]
		message := module.stringify(reqEntry, module.id, module.relogioLoc)
		leadingSpaces = strings.Repeat(" ", 22-len(message))
		module.sendToLink(pl, message, leadingSpaces)
	}
	module.estadoAtual = wantMX
}

func (module *DIMEX_Module) handleUponReqExit() {
	/*
		upon event [ dmx, Exit  |  r  ]  do
			para todo [p, r, ts ] em waiting
				trigger [ pl, Send | p , [ respOk, r ]  ]
			estado := naoQueroSC
			waiting := {}
	*/
	fmt.Println("===== handleUponReqExit =====", module.id)
	for i := 0; i < len(module.waiting); i++ {
		if module.waiting[i] {
			pl := module.addresses[i]
			message := module.stringify(respOk, module.id, module.relogioLoc)
			leadingSpaces := strings.Repeat(" ", 22-len(message))
			module.sendToLink(pl, message, leadingSpaces)
			module.waiting[i] = false
		}
	}
	module.estadoAtual = outMX
}

// ------------------------------------------------------------------------------------
// ------- tratamento de mensagens de outros processos
// ------- UPON respOK
// ------- UPON reqEntry
// ------------------------------------------------------------------------------------

func (module *DIMEX_Module) handleUponDeliverRespOk(msgOutro PP2PLink.PP2PLink_Ind_Message) {
	/*
		upon event [ pl, Deliver | p, [ respOk, r ] ]
			resps++
			se resps = N
			então trigger [ dmx, Deliver | free2Access ]
				estado := estouNaSC
	*/
	fmt.Println("===== handleUponDeliverRespOk =====", module.id)
	module.numeroRespostasRecebidas++
	N := len(module.addresses) -1
	fmt.Println("numeroRespostasRecebidas: ", module.numeroRespostasRecebidas, " N: ", N)
	if module.numeroRespostasRecebidas == N {
		module.Ind <- dmxResp{}
		module.estadoAtual = inMX
	}
}

func (module *DIMEX_Module) handleUponDeliverReqEntry(msgOutro PP2PLink.PP2PLink_Ind_Message) {
	// outro processo quer entrar na SC
	/*
		upon event [ pl, Deliver |p , [ reqEntry, r, rts ]  do
			se (estado == naoQueroSC)   OR
					(estado == QueroSC AND  myTs >  ts)
			então  trigger [ pl, Send | p , [ respOk, r ]  ]
			senão
				se (estado == estouNaSC) OR
						(estado == QueroSC AND  myTs < ts)
				então  postergados := postergados + [p, r ]
				lts.ts := max(lts.ts, rts.ts)
	*/
	fmt.Println("===== handleUponDeliverReqEntry =====", module.id)
	_, idOutro, relogioOutro := module.parse(msgOutro.Message)
	if module.estadoAtual == outMX || (module.estadoAtual == wantMX && before(idOutro, relogioOutro, module.id, module.relogioLoc)) {
		pl := module.addresses[idOutro]
		message := module.stringify(respOk, module.id, module.relogioLoc)
		leadingSpaces := strings.Repeat(" ", 22-len(message))
		module.sendToLink(pl, message, leadingSpaces)
	} else if module.estadoAtual == inMX || (module.estadoAtual == wantMX && before(module.id, module.relogioLoc, idOutro, relogioOutro)) {
		module.waiting[idOutro] = true
	} else {
		fmt.Println("\033[1;31m === Meia-volta meu chapa : ( === \033[0m")
	}
	if relogioOutro > module.relogioLoc {
		module.relogioLoc = relogioOutro
	}
}

// ------------------------------------------------------------------------------------
// ------- funcoes de ajuda
// ------------------------------------------------------------------------------------

func (module *DIMEX_Module) sendToLink(address string, content string, space string) {
	module.outDbg(space + " ---->>>>   to: " + address + "     msg: " + content)
	module.Pp2plink.Req <- PP2PLink.PP2PLink_Req_Message{
		To:      address,
		Message: content}
}

func before(oneId, oneTs, othId, othTs int) bool {
	if oneTs < othTs {
		return true
	} else if oneTs > othTs {
		return false
	} else {
		return oneId < othId
	}
}

func (module *DIMEX_Module) outDbg(s string) {
	if module.isDebugging {
		fmt.Println(". . . . . . . . . . . . [ DIMEX : " + s + " ]")
	}
}

func (module *DIMEX_Module) stringify(_mensagem string, _id int, _relogioLocal int) string {
	id := strconv.Itoa(_id)
	relogioLocal := strconv.Itoa(_relogioLocal)
	return fmt.Sprintf("(%s) %s ts=%s", id, _mensagem, relogioLocal)
}

func (module *DIMEX_Module) parse(msg string) (mensagem string, id int, relogioLocal int) {
	// Returns ("id") string
	id_full := getWord(msg, 0)
	// Returns "id" string
	_id := remove(remove(id_full, "("), ")")
	// Returns id int
	id, _ = strconv.Atoi(_id)

	// Returns text content (respOk, reqSent, etc)
	mensagem = getWord(msg, 1)

	// Returns ts="ts"
	relogioLocal_full := getWord(msg, 2)
	// Returns "ts" string
	_relogioLocal := remove(relogioLocal_full, "ts=")
	// Returns ts int
	relogioLocal, _ = strconv.Atoi(_relogioLocal)

	return mensagem, id, relogioLocal
}

func getWord(str string, index int) string {
	return strings.Split(str, " ")[index]
}

func remove(str, old string) string {
	return strings.Replace(string(str), old, "", -1)
}
