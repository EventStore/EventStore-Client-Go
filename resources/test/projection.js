fromAll()
.when({
	$init:function(){
		return {
			foo: {
				baz: {
					count: 0
				}
			}
		}
	},
	$any: function(state, event){
		state.foo.baz.count += 1;
	}
}).outputState()

