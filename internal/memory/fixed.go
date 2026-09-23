package memory

import (
	"fmt"
	"math"
	"math/big"

	"mahoroba.local/mahoroba/internal/canonical"
)

func multiplyDivideFloor(left, right, divisor int64) (int64, error) {
	if left < 0 || right < 0 || divisor <= 0 {
		return 0, fmt.Errorf("memory: invalid fixed-point operands %d * %d / %d", left, right, divisor)
	}
	product := new(big.Int).Mul(big.NewInt(left), big.NewInt(right))
	product.Quo(product, big.NewInt(divisor))
	if !product.IsInt64() {
		return 0, fmt.Errorf("memory: fixed-point result overflows int64")
	}
	return product.Int64(), nil
}

func weightedRatioFloor(weighted ...int64) (canonical.Ratio, error) {
	if len(weighted)%2 != 0 {
		return 0, fmt.Errorf("memory: weighted ratio requires weight/value pairs")
	}
	total := new(big.Int)
	for index := 0; index < len(weighted); index += 2 {
		weight, value := weighted[index], weighted[index+1]
		if weight < 0 || value < 0 || value > fixedPointScale {
			return 0, fmt.Errorf("memory: invalid weighted ratio pair %d,%d", weight, value)
		}
		term := new(big.Int).Mul(big.NewInt(weight), big.NewInt(value))
		total.Add(total, term)
	}
	total.Quo(total, big.NewInt(fixedPointScale))
	if !total.IsInt64() || total.Int64() > fixedPointScale {
		return 0, fmt.Errorf("memory: weighted ratio is out of range")
	}
	return canonical.NewRatio(total.Int64())
}

func ratioOf(numerator, denominator int64) (canonical.Ratio, error) {
	if numerator < 0 || denominator < 0 || numerator > denominator {
		return 0, fmt.Errorf("memory: invalid ratio %d/%d", numerator, denominator)
	}
	if denominator == 0 {
		return canonical.NewRatio(0)
	}
	value, err := multiplyDivideFloor(numerator, fixedPointScale, denominator)
	if err != nil {
		return 0, err
	}
	return canonical.NewRatio(value)
}

func checkedAdd(left, right int64) (int64, error) {
	if right > 0 && left > math.MaxInt64-right {
		return 0, fmt.Errorf("memory: integer addition overflows")
	}
	if right < 0 && left < math.MinInt64-right {
		return 0, fmt.Errorf("memory: integer addition underflows")
	}
	return left + right, nil
}
