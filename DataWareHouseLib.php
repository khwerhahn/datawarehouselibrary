<?php

/// Carbon is a dependency

///// Tables

// CREATE TABLE datawarehouse(
//    ID  bigserial PRIMARY KEY,
//    query           TEXT      NOT NULL,
//    hash            CHAR(64)     NOT NULL,
//    value        decimal NOT NULL,
//    created_at         TIMESTAMP NOT NULL,
// 	 updated_at TIMESTAMP NOT NULL,
// 	 invalid_until TIMESTAMP NOT NULL
// );

class DataWareHouseLib {
	
	private $status = true;

	private function hashit($string){
		//// HASH sha256 should be enough for queries not to produce collisions
		return hash('sha256', $string);

	}

	private function makeCarbon($date){
		return new Carbon($date);
	}

	public function get($query){

		$value = array('status' => false, 'value' => 0, 'last_updated' => NULL, 'valid' => false);

		// [1] Hash Query
		$hashed = $this->hashit($query);

		// [2] Look if exist in DB
		$lookup = DB::table('datawarehouse')->where('hash', $hashed)->get();

		// [3] Evaluate Lookup
		if(count($lookup) === 1){
			// query found
			$value['status'] = true;
			$value['value'] = (float)$lookup[0]->value;
			$value['last_updated'] = $this->makeCarbon($lookup[0]->updated_at);
			$invalid_until = $this->makeCarbon($lookup[0]->invalid_until);
			// compare updated_at and invalid_until to determine if value is clean
			$diffindays = $invalid_until->diffInSeconds($value['last_updated']);
			if($diffindays === 0){
				$value['valid'] = true;
			}


		}elseif(count($lookup) > 1){
			// error --- shouldn't happen
			throw new Exception('Hash found multiple times!');
		}
		/// no else needed (stadard return value already set)

		return $value;

	}

	public function store($query,$value,$invalid_until){

		// [1] Run validations
		if(empty($query)){
			throw new Exception('Missing query');
		}
		elseif(strlen($query) < 6){
			throw new Exception('Invalid query');
		}

		if(empty($value)){
			$value = 0;
		}

		if($this->status === true){
			// [2] Hash Query
			$hashed = $this->hashit($query);
			// [3] Store
			try{
				$store = DB::table('datawarehouse')
				->insert(
					array(
						'query' => $query,
						'hash' => $hashed,
						'value' => $value,
						'created_at' => new DateTime,
						'invalid_until' => $invalid_until,
						'updated_at' => new DateTime
					)
				);	
			}
			catch (Exception $e) {
			  throw new Exception($e->getMessage());
			}
			
		}else{
			throw new Exception('Validation failed!');
		}
		


	}

	public function update($take){

		/// get entries from database that are not valid yet
		$getinvalid = DB::select("select id, query, invalid_until ,(SELECT EXTRACT(EPOCH FROM (invalid_until - updated_at))) as diff from datawarehouse where updated_at < invalid_until order by diff desc limit ".$take."");
		
		foreach($getinvalid as $key => $value){
			$getnewvalue = DB::select($value->query);
			if(isset($getnewvalue[0]->value)){
				$update = DB::table('datawarehouse')->where('id', $value->id)->update(array('value' => $getnewvalue[0]->value, 'updated_at' => new DateTime ));
			}
		}


	}

}