/*
 * cfg_tree_handlers.hpp
 *
 * Copyright (C) 2025 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * This program is free software: you can redistribute it and/or modify it under
 * the terms of the GNU Affero General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses/
 */

//==========================================================
// Includes.
//

#pragma once

#include "nlohmann/json.hpp"

extern "C" {
#include "base/cfg.h"
}


//==========================================================
// Public API.
//

using json = nlohmann::json;

namespace cfg_handlers {

	//==========================================================
	// Typedefs & constants & classes.
	//

	// Custom exception for configuration errors
	class config_error : public std::runtime_error {
	public:
		config_error(const std::string& field_path, const std::string& message)
			: std::runtime_error("Field " + field_path + ": " + message)
			, field_path_(field_path) {}
		
		const std::string& field_path() const { return field_path_; }
		
	private:
		std::string field_path_;
	};

	//==========================================================
	// Public API.
	//

	void apply_config(as_config* config, const nlohmann::json& source);
}