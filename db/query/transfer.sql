-- name: CreateTransfer :one
INSERT INTO transfer (
    from_account_id,
    to_account_id,
    amount
) VALUES (
             $1, $2, $3
         )
    RETURNING *;

-- name: GetTransfer :one
SELECT * FROM transfer
WHERE id = $1 LIMIT 1;



-- name: DeleteTransfer :exec
DELETE FROM transfer WHERE id = $1;