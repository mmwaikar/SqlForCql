(ns sqlforcql.atoms-test)

(def players-table (atom "players"))
(def players-by-city-table (atom "players_by_city"))

(def players-pk-col (atom :nickname))
(def players-non-pk-col (atom :city))

(def players-by-city-pk-col (atom :city))
(def players-by-city-ck-col (atom :country))
(def players-by-city-pk-ck-cols (atom [:city :country]))
(def players-by-city-non-pk-col (atom :zip))