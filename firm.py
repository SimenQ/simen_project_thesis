import numpy as np
import pandas as pd
class Firm:
    def __init__(self,name, industry, stock_price, forecasted_eps_1, forecasted_eps_2, tax_rate, dividend_payout_ratio, total_assets, shares_outstanding, industry_roe,interest_expense, interest_bearing_debt,total_capital,total_equity,total_debt,forecasted_eps_3=None,ltg=None):
        self.name = name
        self.industry = industry
        self.stock_price = stock_price

        if np.isnan(dividend_payout_ratio):
            self.dividend_payout_ratio = 0
        else:
            self.dividend_payout_ratio = dividend_payout_ratio / 100

        self.industry_roe = industry_roe/100
        self.interest_bearing_debt = interest_bearing_debt
        self.shares_outstanding = shares_outstanding 
        self.total_assets = total_assets
        self.total_equity = total_equity
        self.total_debt = total_debt
        self.forecasted_eps_1 = forecasted_eps_1
        self.forecasted_eps_2 = forecasted_eps_2
        self.forecasted_eps_3 = forecasted_eps_3
        self.forecasted_eps = self.get_forecasted_numbers()

        if ltg is not None:
            self.ltg = ltg/100
        else:
            self.ltg = None

        self.interest_expense = interest_expense
        self.total_capital = total_capital
        self.tax_rate = tax_rate/100

    def get_forecasted_numbers(self):
        if self.forecasted_eps_3 == None:
            forecasted_eps = [self.forecasted_eps_1, self.forecasted_eps_2]
        else: 
            forecasted_eps = [self.forecasted_eps_1, self.forecasted_eps_2, self.forecasted_eps_3]
        return forecasted_eps
    
    def calulate_weight_of_equity(self):
        return self.total_equity/self.total_assets
    
    def calculate_weight_of_debt(self):
        if self.total_debt == 0:
            return 0
        return self.total_debt/self.total_assets

    def calculate_book_value_per_share(self):
        if self.total_assets is None or self.total_debt is None or self.shares_outstanding is None:
            return self.bvps
        return (self.total_assets - self.total_debt) / self.shares_outstanding
    
    def calculate_cost_of_debt(self):
        if self.interest_expense == 0 or self.interest_bearing_debt == 0:
            return 0
        return self.interest_expense/self.interest_bearing_debt

    def extend_forecasted_eps(self):
        if len(self.forecasted_eps) < 2:
            raise ValueError("At least two years of EPS forecasts are needed to estimate growth rates.")
        
        while len(self.forecasted_eps) < 3:
            if self.ltg is not None:
                self.forecasted_eps.append(self.forecasted_eps[-1] * (1 + self.ltg))
            else:
                # Calculate growth rates considering positive to negative transitions
                growth_rates = []
                for i in range(1, len(self.forecasted_eps)):
                    prev_eps = self.forecasted_eps[i - 1]
                    current_eps = self.forecasted_eps[i]

                    if prev_eps > 0 > current_eps:
                        # Transition from profit to loss
                        growth_rate = -1  # Interpretation: 100% decline or more
                    elif prev_eps < 0:
                        # Negative to negative transition
                        if current_eps < prev_eps:
                            # Increasing losses
                            growth_rate = (prev_eps - current_eps) / abs(prev_eps)
                        else:
                            # Decreasing losses or return to profit
                            growth_rate = (current_eps - prev_eps) / abs(prev_eps)
                    else:
                        # Standard case: both EPS are positive or negative to positive transition
                        growth_rate = (current_eps / prev_eps) - 1

                    growth_rates.append(growth_rate)
                
                avg_growth_rate = sum(growth_rates) / len(growth_rates)
                self.forecasted_eps.append(self.forecasted_eps[-1] * (1 + avg_growth_rate))



    def get_forecasted_roes(self,forecasted_bvps = None):
        fading_period_length = 9 
         # From year 3 to 12
        forecasted_roes = [self.forecasted_eps[i] / forecasted_bvps[i] for i in range(len(self.forecasted_eps))]
        extended = np.linspace(forecasted_roes[2], self.industry_roe, fading_period_length)
        return forecasted_roes + list(extended)
    
    def get_book_value_over_time(self):
        forecasted_bvps = [self.calculate_book_value_per_share()]
        for i in range(3): 
            forecasted_bvps.append(forecasted_bvps[-1] + self.forecasted_eps[i] - self.dividend_payout_ratio * self.forecasted_eps[i])
        data = self.get_forecasted_roes(forecasted_bvps)
        for i in range(3,12):
            forecasted_bvps.append(forecasted_bvps[-1] + forecasted_bvps[-1] * (1-self.dividend_payout_ratio) * data[i])
        return forecasted_bvps


    def calculate_gls_cost_of_equity_attempt(self,cost_of_equity):
        self.extend_forecasted_eps()
        price = 0
        price += self.calculate_book_value_per_share()
        book_values = self.get_book_value_over_time()   
        roe_estimates = self.get_forecasted_roes(book_values)
        years = 11
        LARGE_NUMBER = 500000 
        MINIMUM_STOCK_PRICE = 0

        if cost_of_equity < 0.0001:  
            cost_of_equity = 0.0001

        for i in range(years): 
            price += ((roe_estimates[i] - cost_of_equity) / ((1 + cost_of_equity) ** (i + 1))) * book_values[i]

        # Handle division by small cost_of_equity
        final_term = ((roe_estimates[years] - cost_of_equity) / (cost_of_equity * ((1 + cost_of_equity) ** (years - 1)))) * book_values[years - 1]
        if abs(final_term) > LARGE_NUMBER:  
            final_term = LARGE_NUMBER  # Cap the term to prevent extreme values

        price += final_term
        price = max(price, MINIMUM_STOCK_PRICE)
        return price
    
    def find_cost_of_equity(self, initial_guess=0.09, tolerance=0.005, max_iterations=5000, max_iteration_2 = 5000):
        constant = 0.002
        current_guess = initial_guess

        calculated_price = self.calculate_gls_cost_of_equity_attempt(current_guess)
        first_sign = np.sign(calculated_price - self.stock_price)
        sign_new = first_sign
        old_guess = current_guess
        difference = abs(calculated_price - self.stock_price)

        # Adjust the guess until the calculated price crosses the actual price
        iterations = 0 
        while np.sign(calculated_price - self.stock_price) == first_sign and iterations < max_iterations:
            iterations += 1
            old_guess = current_guess
            current_guess += constant * sign_new
            calculated_price = self.calculate_gls_cost_of_equity_attempt(current_guess)
        
        old_guess_oldest = old_guess
        old_guess_newest = current_guess

       # Refine the guess with a smaller adjustment
        iterations_2 = 0 
        while difference > tolerance and iterations_2 < max_iteration_2:
            iterations_2 += 1
            current_guess = (old_guess_oldest + old_guess_newest) / 2
            calculated_price = self.calculate_gls_cost_of_equity_attempt(current_guess)
            difference = abs(calculated_price - self.stock_price)

            if np.sign(calculated_price - self.stock_price) > 0: 
                old_guess_newest_copy = old_guess_newest
                old_guess_newest = current_guess
                old_guess_oldest = max(old_guess_oldest, old_guess_newest_copy)
            else:
                old_guess_oldest_copy = old_guess_oldest
                old_guess_oldest = current_guess
                old_guess_newest = min(old_guess_newest, old_guess_oldest_copy)

        if iterations >= max_iterations or iterations_2 > max_iteration_2:
            print("Warning: Maximum iterations reached for firm", self.name, ". Skipping...")
            return None
        else:
            #print("Cost of equity for firm", self.name, ":", current_guess)
            return current_guess
        
   

    def calculate_gls_cost_of_capital(self):
        cost_of_debt = self.calculate_cost_of_debt()
        weight_of_debt = self.calculate_weight_of_debt()
        tax_rate = self.tax_rate
        cost_of_equity = self.find_cost_of_equity()

        # Check if cost_of_equity is None
        if cost_of_equity is None:
            print(f"Warning: Unable to calculate cost of equity for {self.name}.")
            return None
           
        weight_of_equity = self.calulate_weight_of_equity()
        
        # Calculate the cost of capital
        return cost_of_debt * weight_of_debt * (1 - tax_rate) + cost_of_equity * weight_of_equity
