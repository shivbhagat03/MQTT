from productionval import ProductionCalculator

def main():
    calculator = ProductionCalculator()
    
    try:
        machineId = "BAY_1_900_2"
        start_time = "2025-01-29 09:00:00"
        end_time = "2025-01-29 17:00:00"

        result = calculator.calculate_production(start_time, end_time, machineId)

        if "error" in result:
            print(f"Error: {result['error']}")
        else:
            print("\nProduction Value Calculation Results:")
            print(f"Machine ID: {result['machineId']}")
            print(f"Time Range: {result['start_time']} to {result['end_time']}")
            print(f"\nProduction Value: {result['production_value']} strokes")
    
    except Exception as e:
        print(f"Error in main: {str(e)}")
    finally:
        calculator.close()

if __name__ == "__main__":
    main()