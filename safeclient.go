package main

import (
	"context"
	"errors"
	"math/big"
	"sync"

	"fmt"

	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/log"
)

var errNotConnectd = errors.New("blockchain client not connected")

// Status shows actual connection status.
type Status int

const (
	//Disconnected init status
	Disconnected = Status(iota)
	//Connected connection status
	Connected
	//Closed user closed
	Closed
	//Reconnecting connection error
	Reconnecting
)

//SafeEthClient how to recover from a restart of geth
type SafeEthClient struct {
	*ethclient.Client
	lock       sync.Mutex
	url        string
	ReConnect  map[string]chan struct{}
	Status     Status
	StatusChan chan Status
	quitChan   chan struct{}
}

//NewSafeClient create safeclient
func NewSafeClient(rawurl string) (*SafeEthClient, error) {
	c := &SafeEthClient{
		ReConnect:  make(map[string]chan struct{}),
		url:        rawurl,
		StatusChan: make(chan Status, 10),
		quitChan:   make(chan struct{}),
	}
	var err error
	c.Client, err = ethclient.Dial(rawurl)
	if err == nil && checkConnectStatus(c.Client) == nil {
		c.changeStatus(Connected)
	} else {
		go c.RecoverDisconnect()
	}
	return c, nil
}

//Close connection when destroy atmosphere service
func (c *SafeEthClient) Close() {
	if c.Client != nil {
		c.Client.Close()
		c.changeStatus(Closed)
	}
	close(c.quitChan)
}

//IsConnected return true when connected to eth rpc server
func (c *SafeEthClient) IsConnected() bool {
	return c.Status == Connected
}

//RegisterReConnectNotify register notify when reconnect
func (c *SafeEthClient) RegisterReConnectNotify(name string) <-chan struct{} {
	c.lock.Lock()
	defer c.lock.Unlock()
	ch, ok := c.ReConnect[name]
	if ok {
		log.Warn("NeedReConnectNotify should only call once")
		return ch
	}
	ch = make(chan struct{}, 1)
	c.ReConnect[name] = ch
	return ch
}
func (c *SafeEthClient) changeStatus(newStatus Status) {
	log.Info(fmt.Sprintf("ethclient connection status changed from %d to %d", c.Status, newStatus))
	c.Status = newStatus
	select {
	case c.StatusChan <- c.Status:
	default:
		//never block
	}
}

//RecoverDisconnect try to reconnect with geth after a restart of geth
func (c *SafeEthClient) RecoverDisconnect() {
	var err error
	var client *ethclient.Client
	c.changeStatus(Reconnecting)
	if c.Client != nil {
		c.Client.Close()
	}
	for {
		log.Info("tyring to reconnect geth ...")
		select {
		case <-c.quitChan:
			return
		default:
			//never block
		}
		client, err = ethclient.Dial(c.url)
		if err == nil {
			err = checkConnectStatus(client)
		}
		if err == nil {
			//reconnect ok
			c.Client = client
			c.changeStatus(Connected)
			c.lock.Lock()
			var keys []string
			for name, c := range c.ReConnect {
				keys = append(keys, name)
				c <- struct{}{}
				close(c)
			}
			for _, name := range keys {
				delete(c.ReConnect, name)
			}
			c.lock.Unlock()
			return
		}
		log.Info(fmt.Sprintf("reconnect to geth error: %s", err))
		time.Sleep(time.Second * 3)
	}
}

func (c *SafeEthClient) BlockByHash(ctx context.Context, hash common.Hash) (r1 *types.Block, err error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	r1, err = c.Client.BlockByHash(ctx, hash)
	return
}

func (c *SafeEthClient) BlockByNumber(ctx context.Context, number *big.Int) (*types.Block, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.Client == nil {
		return nil, errNotConnectd
	}
	return c.Client.BlockByNumber(ctx, number)
}

func (c *SafeEthClient) HeaderByHash(ctx context.Context, hash common.Hash) (*types.Header, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.Client == nil {
		return nil, errNotConnectd
	}
	return c.Client.HeaderByHash(ctx, hash)
}

func (c *SafeEthClient) HeaderByNumber(ctx context.Context, number *big.Int) (*types.Header, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.Client == nil {
		return nil, errNotConnectd
	}
	return c.Client.HeaderByNumber(ctx, number)
}

func (c *SafeEthClient) TransactionByHash(ctx context.Context, hash common.Hash) (tx *types.Transaction, isPending bool, err error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.Client == nil {
		return nil, false, errNotConnectd
	}
	return c.Client.TransactionByHash(ctx, hash)
}

func (c *SafeEthClient) TransactionSender(ctx context.Context, tx *types.Transaction, block common.Hash, index uint) (common.Address, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.Client == nil {
		return common.Address{}, errNotConnectd
	}
	return c.Client.TransactionSender(ctx, tx, block, index)
}

func (c *SafeEthClient) TransactionCount(ctx context.Context, blockHash common.Hash) (uint, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.Client == nil {
		return 0, errNotConnectd
	}
	return c.Client.TransactionCount(ctx, blockHash)
}

func (c *SafeEthClient) TransactionInBlock(ctx context.Context, blockHash common.Hash, index uint) (*types.Transaction, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.Client == nil {
		return nil, errNotConnectd
	}
	return c.Client.TransactionInBlock(ctx, blockHash, index)
}

func (c *SafeEthClient) TransactionReceipt(ctx context.Context, txHash common.Hash) (*types.Receipt, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.Client == nil {
		return nil, errNotConnectd
	}
	return c.Client.TransactionReceipt(ctx, txHash)
}

func (c *SafeEthClient) SyncProgress(ctx context.Context) (*ethereum.SyncProgress, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.Client == nil {
		return nil, errNotConnectd
	}
	return c.Client.SyncProgress(ctx)
}

func (c *SafeEthClient) SubscribeNewHead(ctx context.Context, ch chan<- *types.Header) (ethereum.Subscription, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.Client == nil {
		return nil, errNotConnectd
	}
	return c.Client.SubscribeNewHead(ctx, ch)
}

func (c *SafeEthClient) NetworkID(ctx context.Context) (*big.Int, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.Client == nil {
		return nil, errNotConnectd
	}
	return c.Client.NetworkID(ctx)
}

func (c *SafeEthClient) BalanceAt(ctx context.Context, account common.Address, blockNumber *big.Int) (*big.Int, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.Client == nil {
		return nil, errNotConnectd
	}
	return c.Client.BalanceAt(ctx, account, blockNumber)
}

func (c *SafeEthClient) StorageAt(ctx context.Context, account common.Address, key common.Hash, blockNumber *big.Int) ([]byte, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.Client == nil {
		return nil, errNotConnectd
	}
	return c.Client.StorageAt(ctx, account, key, blockNumber)
}

func (c *SafeEthClient) CodeAt(ctx context.Context, account common.Address, blockNumber *big.Int) ([]byte, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.Client == nil {
		return nil, errNotConnectd
	}
	return c.Client.CodeAt(ctx, account, blockNumber)
}

func (c *SafeEthClient) NonceAt(ctx context.Context, account common.Address, blockNumber *big.Int) (uint64, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.Client == nil {
		return 0, errNotConnectd
	}
	return c.Client.NonceAt(ctx, account, blockNumber)
}

func (c *SafeEthClient) FilterLogs(ctx context.Context, q ethereum.FilterQuery) ([]types.Log, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.Client == nil {
		return nil, errNotConnectd
	}
	return c.Client.FilterLogs(ctx, q)
}

func (c *SafeEthClient) SubscribeFilterLogs(ctx context.Context, q ethereum.FilterQuery, ch chan<- types.Log) (ethereum.Subscription, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.Client == nil {
		return nil, errNotConnectd
	}
	return c.Client.SubscribeFilterLogs(ctx, q, ch)
}

func (c *SafeEthClient) PendingBalanceAt(ctx context.Context, account common.Address) (*big.Int, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.Client == nil {
		return nil, errNotConnectd
	}
	return c.Client.PendingBalanceAt(ctx, account)
}

func (c *SafeEthClient) PendingStorageAt(ctx context.Context, account common.Address, key common.Hash) ([]byte, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.Client == nil {
		return nil, errNotConnectd
	}
	return c.Client.PendingStorageAt(ctx, account, key)
}

func (c *SafeEthClient) PendingCodeAt(ctx context.Context, account common.Address) ([]byte, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.Client == nil {
		return nil, errNotConnectd
	}
	return c.Client.PendingCodeAt(ctx, account)
}

func (c *SafeEthClient) PendingNonceAt(ctx context.Context, account common.Address) (nonce uint64, err error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.Client == nil {
		return 0, errNotConnectd
	}
	nonce, err = c.Client.PendingNonceAt(ctx, account)
	return
}

func (c *SafeEthClient) PendingTransactionCount(ctx context.Context) (uint, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.Client == nil {
		return 0, errNotConnectd
	}
	return c.Client.PendingTransactionCount(ctx)
}

func (c *SafeEthClient) CallContract(ctx context.Context, msg ethereum.CallMsg, blockNumber *big.Int) ([]byte, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.Client == nil {
		return nil, errNotConnectd
	}
	return c.Client.CallContract(ctx, msg, blockNumber)
}

func (c *SafeEthClient) PendingCallContract(ctx context.Context, msg ethereum.CallMsg) ([]byte, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.Client == nil {
		return nil, errNotConnectd
	}
	return c.Client.PendingCallContract(ctx, msg)
}

func (c *SafeEthClient) SuggestGasPrice(ctx context.Context) (*big.Int, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.Client == nil {
		return nil, errNotConnectd
	}
	return c.Client.SuggestGasPrice(ctx)
}

func (c *SafeEthClient) EstimateGas(ctx context.Context, msg ethereum.CallMsg) (uint64, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.Client == nil {
		return 0, errNotConnectd
	}
	return c.Client.EstimateGas(ctx, msg)
}

func (c *SafeEthClient) SendTransaction(ctx context.Context, tx *types.Transaction) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.Client == nil {
		return errNotConnectd
	}
	return c.Client.SendTransaction(ctx, tx)
}

func (c *SafeEthClient) GenesisBlockHash(ctx context.Context) (genesisBlockHash common.Hash, err error) {

	c.lock.Lock()
	defer c.lock.Unlock()
	if c.Client == nil {
		return common.Hash{}, errNotConnectd
	}
	genesisBlockHead, err := c.Client.HeaderByNumber(ctx, big.NewInt(1))
	if err != nil {
		return
	}
	return genesisBlockHead.Hash(), nil
}

func checkConnectStatus(c *ethclient.Client) (err error) {
	if c == nil {
		return errNotConnectd
	}
	ctx, cancelFunc := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelFunc()
	_, err = c.HeaderByNumber(ctx, big.NewInt(1))
	if err != nil {
		return
	}
	return
}
