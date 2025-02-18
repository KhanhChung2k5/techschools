package db

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestTransferTx(t *testing.T) {
	existed := make(map[int]bool)

	store := NewStore(testDB)

	n := 5
	amount := int64(10)

	for i := 0; i < n; i++ {
		account1 := createRandomAccount(t)
		account2 := createRandomAccount(t)
		fmt.Println(">> before:", account1.Balance, account2.Balance)
		result, err := store.TransferTx(context.Background(), TransferTxParams{
			FromAccountID: account1.ID,
			ToAccountID:   account2.ID,
			Amount:        amount,
		})

		require.NoError(t, err)

		require.NotEmpty(t, result)

		//check transfer
		transfer := result.Transfer
		require.NotEmpty(t, transfer)
		require.Equal(t, sql.NullInt64{Int64: account1.ID, Valid: true}, transfer.FromAccountID)
		require.Equal(t, sql.NullInt64{Int64: account2.ID, Valid: true}, transfer.ToAccountID)
		require.Equal(t, amount, transfer.Amount)
		require.NotZero(t, transfer.ID)
		require.NotZero(t, transfer.CreatedAt)

		_, err = store.GetTransfer(context.Background(), transfer.ID)
		require.NoError(t, err)

		//check entries
		fromEntry := result.FromEntry
		require.NotEmpty(t, fromEntry)
		require.Equal(t, sql.NullInt64{Int64: account1.ID, Valid: true}, fromEntry.AccountID)
		require.Equal(t, -amount, fromEntry.Amount)
		require.NotZero(t, fromEntry.ID)
		require.NotZero(t, fromEntry.CreatedAt)

		_, err = store.GetEntry(context.Background(), fromEntry.ID)
		require.NoError(t, err)

		toEntry := result.ToEntry
		require.NotEmpty(t, toEntry)
		require.Equal(t, sql.NullInt64{Int64: account2.ID, Valid: true}, toEntry.AccountID)
		require.Equal(t, amount, toEntry.Amount)
		require.NotZero(t, toEntry.ID)
		require.NotZero(t, toEntry.CreatedAt)

		_, err = store.GetEntry(context.Background(), toEntry.ID)
		require.NoError(t, err)

		//check account
		fromAccount := result.FromAccount
		require.NotEmpty(t, fromAccount)
		require.NotEqual(t, sql.NullInt64{Int64: account1.ID, Valid: true}, fromAccount.ID)

		toAccount := result.ToAccount
		require.NotEmpty(t, toAccount)
		require.NotEqual(t, sql.NullInt64{Int64: account2.ID, Valid: true}, toAccount.ID)

		//check account balance
		fmt.Println(">> after:", fromAccount.Balance, toAccount.Balance)

		diff1 := account1.Balance - fromAccount.Balance
		diff2 := toAccount.Balance - account2.Balance
		require.Equal(t, diff1, diff2)
		require.True(t, diff1 > 0)
		require.True(t, diff1%amount == 0) //amount, 2 * amount, 3 * amount

		k := int(diff1 / amount)
		require.True(t, k >= 1 && k <= n)
		existed[k] = true
	}

	//updateAccount1, err := testQueries.GetAccount(context.Background(), account1.ID)
	//require.NoError(t, err)
	//
	//updateAccount2, err := testQueries.GetAccount(context.Background(), account2.ID)
	//require.NoError(t, err)
	//
	//fmt.Println(">> after:", updateAccount1.Balance, updateAccount2.Balance)
	//
	//require.Equal(t, account1.Balance-int64(n)*amount, updateAccount1.Balance)
	//require.Equal(t, account1.Balance+int64(n)*amount, updateAccount2.Balance)

}

func TestTransferTxDeadlock(t *testing.T) {

	store := NewStore(testDB)
	account1 := createRandomAccount(t)
	account2 := createRandomAccount(t)
	fmt.Println(">> before:", account1.Balance, account2.Balance)

	n := 10
	amount := int64(10)

	for i := 0; i < n; i++ {
		fromAccountID := account1.ID
		toAccountID := account2.ID

		if i%2 == 1 {
			fromAccountID = account2.ID
			toAccountID = account1.ID
		}
		result, err := store.TransferTx(context.Background(), TransferTxParams{
			FromAccountID: fromAccountID,
			ToAccountID:   toAccountID,
			Amount:        amount,
		})

		require.NoError(t, err)

		require.NotEmpty(t, result)

		//check transfer
		transfer := result.Transfer
		require.NotEmpty(t, transfer)
		require.Equal(t, sql.NullInt64{Int64: fromAccountID, Valid: true}, transfer.FromAccountID)
		require.Equal(t, sql.NullInt64{Int64: toAccountID, Valid: true}, transfer.ToAccountID)
		require.Equal(t, amount, transfer.Amount)
		require.NotZero(t, transfer.ID)
		require.NotZero(t, transfer.CreatedAt)

		_, err = store.GetTransfer(context.Background(), transfer.ID)
		require.NoError(t, err)

		//check entries
		fromEntry := result.FromEntry
		require.NotEmpty(t, fromEntry)
		require.Equal(t, sql.NullInt64{Int64: fromAccountID, Valid: true}, fromEntry.AccountID)
		require.Equal(t, -amount, fromEntry.Amount)
		require.NotZero(t, fromEntry.ID)
		require.NotZero(t, fromEntry.CreatedAt)

		_, err = store.GetEntry(context.Background(), fromEntry.ID)
		require.NoError(t, err)

		toEntry := result.ToEntry
		require.NotEmpty(t, toEntry)
		require.Equal(t, sql.NullInt64{Int64: toAccountID, Valid: true}, toEntry.AccountID)
		require.Equal(t, amount, toEntry.Amount)
		require.NotZero(t, toEntry.ID)
		require.NotZero(t, toEntry.CreatedAt)

		_, err = store.GetEntry(context.Background(), toEntry.ID)
		require.NoError(t, err)

		//check account
		fromAccount := result.FromAccount
		require.NotEmpty(t, fromAccount)
		require.NotEqual(t, sql.NullInt64{Int64: fromAccountID, Valid: true}, fromAccount.ID)

		toAccount := result.ToAccount
		require.NotEmpty(t, toAccount)
		require.NotEqual(t, sql.NullInt64{Int64: toAccountID, Valid: true}, toAccount.ID)

		//check account balance
		fmt.Println(">> after:", fromAccount.Balance, toAccount.Balance)

		//diff1 := fromAccount.Balance - fromAccount.Balance
		//diff2 := toAccount.Balance - account2.Balance
		//require.Equal(t, diff1, diff2)
		//require.True(t, diff1 > 0)
		//require.True(t, diff1%amount == 0) //amount, 2 * amount, 3 * amount
		//
		//k := int(diff1 / amount)
		//require.True(t, k >= 1 && k <= n)
	}

	//updateAccount1, err := testQueries.GetAccount(context.Background(), account1.ID)
	//require.NoError(t, err)
	//
	//updateAccount2, err := testQueries.GetAccount(context.Background(), account2.ID)
	//require.NoError(t, err)
	//
	//fmt.Println(">> after:", updateAccount1.Balance, updateAccount2.Balance)
	//
	//require.Equal(t, account1.Balance-int64(n)*amount, updateAccount1.Balance)
	//require.Equal(t, account1.Balance+int64(n)*amount, updateAccount2.Balance)

}
